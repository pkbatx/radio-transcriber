package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	ftpserver "github.com/fclairamb/ftpserverlib"
	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
	"github.com/spf13/afero"
	_ "modernc.org/sqlite"
)

type Transcription struct {
	Timestamp     time.Time
	FileName      string
	OriginalText  string
	CorrectedText string
	Source        string
	Metadata      StreamMetadata
}

type PageData struct {
	Transcriptions []Transcription
	Total          int
	LastUpdated    time.Time
	UploadDir      string
}

type ChannelConfig struct {
	Name                   string `json:"name"`
	Platform               string `json:"platform"`
	BotID                  string `json:"botId"`
	WebhookURL             string `json:"webhookUrl"`
	MessageSuffix          string `json:"messageSuffix"`
	SendUploadNotification bool   `json:"sendUploadNotification"`
	SendTranscription      bool   `json:"sendTranscription"`
	IncludeAudioLink       bool   `json:"includeAudioLink"`
}

type Config struct {
	Environment            string          `json:"environment"`
	WatchDir               string          `json:"watchDir"`
	UploadDir              string          `json:"uploadDir"`
	DatabasePath           string          `json:"databasePath"`
	OpenAIAPIKey           string          `json:"openaiApiKey"`
	GroupmeBotID           string          `json:"groupmeBotId"`
	GroupmeWebhookURL      string          `json:"groupmeWebhookUrl"`
	DiscordWebhookURL      string          `json:"discordWebhookUrl"`
	GenericWebhookURL      string          `json:"genericWebhookUrl"`
	WebhookURL             string          `json:"webhookUrl"`
	SendUploadNotification bool            `json:"sendUploadNotification"`
	SendTranscription      bool            `json:"sendTranscription"`
	IncludeAudioLink       bool            `json:"includeAudioLink"`
	ProcessingDelay        int             `json:"processingDelay"`
	MaxMessageLength       int             `json:"maxMessageLength"`
	WebServerPort          string          `json:"webServerPort"`
	OpenAIModel            string          `json:"openaiModel"`
	GroupmeMessageSuffix   string          `json:"groupmeMessageSuffix"`
	OpenAITranscription    string          `json:"openaiTranscriptionModel"`
	Channels               []ChannelConfig `json:"channels"`
	FTPEnabled             bool            `json:"ftpEnabled"`
	FTPPort                string          `json:"ftpPort"`
	FTPUser                string          `json:"ftpUser"`
	FTPPassword            string          `json:"ftpPassword"`
	PreferredNoiseFilter   string          `json:"preferredNoiseFilter"`
}

type StreamMetadata struct {
	RawTags       map[string]string
	SelectedTag   string
	NormalizedTag string
	ReceivedAt    time.Time
}

var (
	configFilePath      = "config.json"
	config              Config
	configMux           sync.RWMutex
	processedFiles      = make(map[string]bool)
	processedFilesMux   sync.Mutex
	tmpl                *template.Template
	transcriptions      []Transcription
	transcriptionsMux   sync.Mutex
	maxTranscriptions   = 100 // Adjust as needed
	uploadWatcherCancel context.CancelFunc
	uploadWatcherWG     sync.WaitGroup
	ftpServerInstance   *ftpserver.FtpServer
	ftpServerMux        sync.Mutex
	dbConn              *sql.DB
	dbMux               sync.RWMutex
)

var defaultNoiseFilters = map[string]string{
	"narrowband":         "highpass=f=300, lowpass=f=3400",
	"hiss_reduction":     "highpass=f=280, lowpass=f=3200, afftdn=nf=-25",
	"aggressive_cleanup": "highpass=f=250, lowpass=f=3000, anlmdn=s=2:p=0.003, afftdn=nf=-30",
	"speech_boost":       "highpass=f=200, lowpass=f=3200, compand=attacks=0.3:decays=0.8:points=-80/-115|-60/-70|-20/-20|0/0",
}

func init() {
	if err := godotenv.Load(); err != nil {
		log.Println("Error loading .env file, using environment variables")
	}
}

func loadConfig() Config {
	cfg := Config{
		Environment:            "dev",
		WatchDir:               "./watched_directory",
		UploadDir:              "./watched_directory",
		DatabasePath:           "transcriptions.db",
		GroupmeWebhookURL:      "https://api.groupme.com/v3/bots/post",
		SendUploadNotification: true,
		SendTranscription:      true,
		IncludeAudioLink:       true,
		ProcessingDelay:        1,
		MaxMessageLength:       1000,
		WebServerPort:          "8080",
		OpenAIModel:            "gpt-4.1",
		GroupmeMessageSuffix:   " - https://calls.sussexcountyalerts.com/",
		OpenAITranscription:    "gpt-4o-mini-transcribe",
		FTPPort:                "2121",
	}

	if data, err := os.ReadFile(configFilePath); err == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Printf("Unable to parse %s: %v", configFilePath, err)
		}
	} else {
		cfg.Environment = firstNonEmpty(os.Getenv("ENVIRONMENT"), cfg.Environment)
		cfg.WatchDir = firstNonEmpty(os.Getenv("WATCH_DIR"), cfg.WatchDir)
		cfg.UploadDir = firstNonEmpty(os.Getenv("UPLOAD_DIR"), cfg.UploadDir)
		cfg.DatabasePath = firstNonEmpty(os.Getenv("DATABASE_PATH"), cfg.DatabasePath)
		cfg.OpenAIAPIKey = firstNonEmpty(os.Getenv("OPENAI_API_KEY"), cfg.OpenAIAPIKey)
		cfg.GroupmeBotID = firstNonEmpty(os.Getenv("GROUPME_BOT_ID"), cfg.GroupmeBotID)
		cfg.GroupmeWebhookURL = firstNonEmpty(os.Getenv("GROUPME_WEBHOOK_URL"), cfg.GroupmeWebhookURL)
		cfg.DiscordWebhookURL = firstNonEmpty(os.Getenv("DISCORD_WEBHOOK_URL"), cfg.DiscordWebhookURL)
		cfg.GenericWebhookURL = firstNonEmpty(os.Getenv("GENERIC_WEBHOOK_URL"), cfg.GenericWebhookURL)
		cfg.WebhookURL = firstNonEmpty(os.Getenv("WEBHOOK_URL"), cfg.WebhookURL)
		if val := os.Getenv("PROCESSING_DELAY"); val != "" {
			if parsed, err := strconv.Atoi(val); err == nil {
				cfg.ProcessingDelay = parsed
			}
		}
		if val := os.Getenv("MAX_MESSAGE_LENGTH"); val != "" {
			if parsed, err := strconv.Atoi(val); err == nil {
				cfg.MaxMessageLength = parsed
			}
		}
		cfg.WebServerPort = firstNonEmpty(os.Getenv("WEB_SERVER_PORT"), cfg.WebServerPort)
		cfg.OpenAIModel = firstNonEmpty(os.Getenv("OPENAI_MODEL"), cfg.OpenAIModel)
		cfg.GroupmeMessageSuffix = firstNonEmpty(os.Getenv("GROUPME_MESSAGE_SUFFIX"), cfg.GroupmeMessageSuffix)
	}

	if cfg.UploadDir == "" {
		cfg.UploadDir = cfg.WatchDir
	}
	if cfg.UploadDir == "" {
		cfg.UploadDir = "./watched_directory"
	}
	if cfg.WatchDir == "" {
		cfg.WatchDir = cfg.UploadDir
	}
	if cfg.WebServerPort == "" {
		cfg.WebServerPort = "8080"
	}
	if cfg.GroupmeWebhookURL == "" {
		cfg.GroupmeWebhookURL = "https://api.groupme.com/v3/bots/post"
	}
	if cfg.WebhookURL == "" {
		cfg.WebhookURL = cfg.GroupmeWebhookURL
	}
	if cfg.OpenAIModel == "" {
		cfg.OpenAIModel = "gpt-4.1"
	}
	if cfg.OpenAITranscription == "" {
		cfg.OpenAITranscription = "gpt-4o-mini-transcribe"
	}
	if cfg.GroupmeMessageSuffix == "" {
		cfg.GroupmeMessageSuffix = " - https://calls.sussexcountyalerts.com/"
	}
	if cfg.FTPPort == "" {
		cfg.FTPPort = "2121"
	}
	if cfg.ProcessingDelay <= 0 {
		cfg.ProcessingDelay = 1
	}
	if cfg.MaxMessageLength <= 0 {
		cfg.MaxMessageLength = 1000
	}

	if cfg.OpenAIAPIKey == "" {
		cfg.OpenAIAPIKey = os.Getenv("OPENAI_API_KEY")
	}
	if cfg.GroupmeBotID == "" {
		cfg.GroupmeBotID = os.Getenv("GROUPME_BOT_ID")
	}

	if len(cfg.Channels) == 0 && cfg.GroupmeBotID != "" {
		cfg.Channels = []ChannelConfig{
			{
				Name:                   "default",
				Platform:               "groupme",
				BotID:                  cfg.GroupmeBotID,
				WebhookURL:             firstNonEmpty(cfg.GroupmeWebhookURL, cfg.WebhookURL),
				MessageSuffix:          cfg.GroupmeMessageSuffix,
				SendUploadNotification: true,
				SendTranscription:      true,
				IncludeAudioLink:       true,
			},
		}
	}

	return cfg
}

func saveConfig(cfg Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configFilePath, data, 0o600)
}

func setConfig(cfg Config) {
	configMux.Lock()
	defer configMux.Unlock()
	config = cfg
}

func getConfig() Config {
	configMux.RLock()
	defer configMux.RUnlock()
	return config
}

func initDatabase(path string) error {
	dbMux.Lock()
	if dbConn != nil {
		_ = dbConn.Close()
		dbConn = nil
	}
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		dbMux.Unlock()
		transcriptionsMux.Lock()
		transcriptions = nil
		transcriptionsMux.Unlock()
		return nil
	}

	if dir := filepath.Dir(trimmed); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			dbMux.Unlock()
			return err
		}
	}

	conn, err := sql.Open("sqlite", trimmed)
	if err != nil {
		dbMux.Unlock()
		return err
	}
	conn.SetMaxOpenConns(1)

	createTable := `CREATE TABLE IF NOT EXISTS transcriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                file_name TEXT,
                original_text TEXT,
                corrected_text TEXT,
                source TEXT,
                raw_tags TEXT,
                selected_tag TEXT,
                normalized_tag TEXT,
                metadata_received_at TEXT
        )`

	if _, err := conn.Exec(createTable); err != nil {
		dbMux.Unlock()
		return fmt.Errorf("unable to initialize database: %w", err)
	}

	dbConn = conn
	dbMux.Unlock()

	return loadTranscriptionsFromDB()
}

func loadTranscriptionsFromDB() error {
	dbMux.RLock()
	conn := dbConn
	dbMux.RUnlock()

	if conn == nil {
		return nil
	}

	rows, err := conn.Query(`SELECT timestamp, file_name, original_text, corrected_text, source, raw_tags, selected_tag, normalized_tag, metadata_received_at FROM transcriptions ORDER BY datetime(timestamp) DESC LIMIT ?`, maxTranscriptions)
	if err != nil {
		return fmt.Errorf("unable to load transcriptions: %w", err)
	}
	defer rows.Close()

	var loaded []Transcription
	for rows.Next() {
		var (
			tsStr      string
			fileName   string
			original   string
			corrected  string
			source     string
			rawTagsStr string
			selected   string
			normalized string
			receivedAt string
		)

		if err := rows.Scan(&tsStr, &fileName, &original, &corrected, &source, &rawTagsStr, &selected, &normalized, &receivedAt); err != nil {
			return fmt.Errorf("unable to scan transcription row: %w", err)
		}

		t := Transcription{FileName: fileName, OriginalText: original, CorrectedText: corrected, Source: source}
		if tsStr != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, tsStr); err == nil {
				t.Timestamp = parsed
			}
		}

		t.Metadata = StreamMetadata{RawTags: make(map[string]string), SelectedTag: selected, NormalizedTag: normalized}
		if receivedAt != "" {
			if parsed, err := time.Parse(time.RFC3339Nano, receivedAt); err == nil {
				t.Metadata.ReceivedAt = parsed
			}
		}

		if rawTagsStr != "" {
			_ = json.Unmarshal([]byte(rawTagsStr), &t.Metadata.RawTags)
		}

		loaded = append(loaded, t)
	}

	for i, j := 0, len(loaded)-1; i < j; i, j = i+1, j-1 {
		loaded[i], loaded[j] = loaded[j], loaded[i]
	}

	transcriptionsMux.Lock()
	transcriptions = loaded
	transcriptionsMux.Unlock()

	return nil
}

func persistTranscription(t Transcription) {
	dbMux.RLock()
	conn := dbConn
	dbMux.RUnlock()

	if conn == nil {
		return
	}

	rawTagsStr := ""
	if len(t.Metadata.RawTags) > 0 {
		if raw, err := json.Marshal(t.Metadata.RawTags); err == nil {
			rawTagsStr = string(raw)
		}
	}

	receivedAt := ""
	if !t.Metadata.ReceivedAt.IsZero() {
		receivedAt = t.Metadata.ReceivedAt.UTC().Format(time.RFC3339Nano)
	}

	_, err := conn.Exec(
		`INSERT INTO transcriptions (timestamp, file_name, original_text, corrected_text, source, raw_tags, selected_tag, normalized_tag, metadata_received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.Timestamp.UTC().Format(time.RFC3339Nano), t.FileName, t.OriginalText, t.CorrectedText, t.Source, rawTagsStr, t.Metadata.SelectedTag, t.Metadata.NormalizedTag, receivedAt,
	)
	if err != nil {
		log.Printf("Failed to persist transcription: %v", err)
	}
}

func startUploadWatcher(dir string) error {
	return startDirectoryWatcher(dir, &uploadWatcherCancel, &uploadWatcherWG, handleUploadFile)
}

func startDirectoryWatcher(dir string, cancelRef *context.CancelFunc, wg *sync.WaitGroup, handler func(string)) error {
	if cancelRef != nil && *cancelRef != nil {
		(*cancelRef)()
		wg.Wait()
	}

	if strings.TrimSpace(dir) == "" {
		return fmt.Errorf("watch directory is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	if cancelRef != nil {
		*cancelRef = cancel
	}

	wg.Add(1)
	go func(path string) {
		defer wg.Done()
		if err := watchDirectory(ctx, path, handler); err != nil {
			log.Println("Directory watcher stopped:", err)
		}
	}(dir)

	return nil
}

type ftpDriver struct {
	fs       afero.Fs
	settings *ftpserver.Settings
	user     string
	pass     string
}

func newFTPDriver(root, user, pass, listenAddr string) *ftpDriver {
	basePath := afero.NewBasePathFs(afero.NewOsFs(), root)

	return &ftpDriver{
		fs:   basePath,
		user: strings.TrimSpace(user),
		pass: pass,
		settings: &ftpserver.Settings{
			ListenAddr: listenAddr,
			Banner:     "FTP drop server ready",
		},
	}
}

func (d *ftpDriver) GetSettings() (*ftpserver.Settings, error) {
	return d.settings, nil
}

func (d *ftpDriver) ClientConnected(_ ftpserver.ClientContext) (string, error) {
	return "Connected to drop server", nil
}

func (d *ftpDriver) ClientDisconnected(_ ftpserver.ClientContext) {}

func (d *ftpDriver) AuthUser(_ ftpserver.ClientContext, user, pass string) (ftpserver.ClientDriver, error) {
	if d.user == "" {
		return d.fs, nil
	}

	if strings.EqualFold(user, d.user) && (d.pass == "" || pass == d.pass) {
		return d.fs, nil
	}

	return nil, fmt.Errorf("invalid credentials")
}

func (d *ftpDriver) GetTLSConfig() (*tls.Config, error) {
	return nil, nil
}

func startFTPServer(cfg Config) error {
	ftpServerMux.Lock()
	defer ftpServerMux.Unlock()

	if ftpServerInstance != nil {
		_ = ftpServerInstance.Stop()
		ftpServerInstance = nil
	}

	if !cfg.FTPEnabled {
		return nil
	}

	port := strings.TrimSpace(cfg.FTPPort)
	if port == "" {
		port = "2121"
	}

	listenAddr := ":" + port
	driver := newFTPDriver(cfg.UploadDir, cfg.FTPUser, cfg.FTPPassword, listenAddr)
	server := ftpserver.NewFtpServer(driver)

	if err := server.Listen(); err != nil {
		return fmt.Errorf("unable to start FTP server: %w", err)
	}

	ftpServerInstance = server

	go func() {
		if err := server.Serve(); err != nil && !errors.Is(err, io.EOF) {
			log.Printf("FTP server stopped: %v", err)
		}
	}()

	log.Printf("FTP drop server listening on %s and writing to %s", server.Addr(), cfg.UploadDir)

	return nil
}

func buildStreamRecorderArgs(streamURL, outputPattern string, segmentSeconds int) []string {
	silenceFilter := fmt.Sprintf("silencedetect=noise=%ddB:d=%.2f", -55, 0.6)

	return []string{
		"-y",
		"-hide_banner",
		"-loglevel", "warning",
		"-reconnect", "1",
		"-reconnect_streamed", "1",
		"-reconnect_delay_max", "4",
		"-i", streamURL,
		"-af", silenceFilter,
		"-c:a", "libmp3lame",
		"-b:a", "128k",
		"-f", "segment",
		"-segment_time", strconv.Itoa(segmentSeconds),
		"-segment_time_delta", "0.5",
		"-reset_timestamps", "1",
		"-write_id3v2", "1",
		"-strftime", "1",
		"-map", "a",
		outputPattern,
	}
}

func normalizeTransmissionLabel(label string) string {
	if label == "" {
		return ""
	}

	replacer := strings.NewReplacer("_", " ", "-", " ", "–", " ", "—", " ")
	cleaned := replacer.Replace(label)
	cleaned = regexp.MustCompile(`\s+`).ReplaceAllString(cleaned, " ")
	cleaned = strings.TrimSpace(cleaned)

	if cleaned == "" {
		return ""
	}

	return strings.ToUpper(cleaned)
}

func formatMessageWithMetadata(message, source string, metadata StreamMetadata) string {
	parts := make([]string, 0, 2)
	if trimmed := strings.TrimSpace(source); trimmed != "" {
		parts = append(parts, strings.ToUpper(trimmed))
	}
	if metadata.NormalizedTag != "" {
		parts = append(parts, metadata.NormalizedTag)
	}

	if len(parts) == 0 {
		return message
	}

	return fmt.Sprintf("[%s] %s", strings.Join(parts, " · "), message)
}

func isAudioSilent(filePath string) (bool, error) {
	cmd := exec.Command("ffmpeg", "-i", filePath, "-af", "volumedetect", "-vn", "-sn", "-dn", "-f", "null", "/dev/null")
	output, err := cmd.CombinedOutput()
	if err != nil && len(output) == 0 {
		return false, fmt.Errorf("ffmpeg volumedetect failed: %w", err)
	}

	outputStr := string(output)
	volumePattern := regexp.MustCompile(`max_volume:\s*([^\s]+) dB`)
	matches := volumePattern.FindStringSubmatch(outputStr)
	if len(matches) < 2 {
		return false, fmt.Errorf("unable to parse max volume from ffmpeg output")
	}

	if strings.EqualFold(matches[1], "-inf") {
		return true, nil
	}

	maxVolume, parseErr := strconv.ParseFloat(matches[1], 64)
	if parseErr != nil {
		return false, fmt.Errorf("unable to parse volume level: %w", parseErr)
	}

	return maxVolume <= -55.0, nil
}

func transcribeAudio(filePath string) (string, error) {
	cfg := getConfig()

	if cfg.OpenAIAPIKey == "" {
		return "", fmt.Errorf("missing OpenAI API key")
	}

	filteredFilePath, err := preprocessAudio(filePath)
	if err != nil {
		return "", fmt.Errorf("audio preprocessing failed: %w", err)
	}
	defer os.Remove(filteredFilePath)

	apiURL := "https://api.openai.com/v1/audio/transcriptions"

	audioFile, err := os.Open(filteredFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open audio file: %w", err)
	}
	defer audioFile.Close()

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	part, err := writer.CreateFormFile("file", filepath.Base(filteredFilePath))
	if err != nil {
		return "", fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err = io.Copy(part, audioFile); err != nil {
		return "", fmt.Errorf("failed to copy audio file: %w", err)
	}

	model := cfg.OpenAITranscription
	if model == "" {
		model = "gpt-4o-mini-transcribe"
	}

	_ = writer.WriteField("model", model)
	_ = writer.WriteField("response_format", "text")
	_ = writer.WriteField("language", "en")

	if err = writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close writer: %w", err)
	}

	req, err := http.NewRequest("POST", apiURL, &requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.OpenAIAPIKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("OpenAI API error: %s", string(bodyBytes))
	}

	transcription := strings.TrimSpace(string(bodyBytes))

	log.Println("Transcription completed for:", filePath)

	return transcription, nil
}

func preprocessAudio(inputFilePath string) (string, error) {
	tempDir := os.TempDir()
	outputFilePath := filepath.Join(tempDir, filepath.Base(inputFilePath)+".filtered.mp3")

	filterChain := resolveNoiseFilter(getConfig())
	log.Printf("Applying noise filter chain '%s' for %s", filterChain, filepath.Base(inputFilePath))
	cmd := exec.Command("ffmpeg", "-y", "-i", inputFilePath, "-af", filterChain, outputFilePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffmpeg error: %s", string(output))
	}

	return outputFilePath, nil
}

func resolveNoiseFilter(cfg Config) string {
	selected := strings.TrimSpace(cfg.PreferredNoiseFilter)
	if selected == "" {
		if chain, ok := defaultNoiseFilters["narrowband"]; ok {
			return chain
		}
		return "highpass=f=300, lowpass=f=3400"
	}

	if chain, ok := defaultNoiseFilters[strings.ToLower(selected)]; ok {
		return chain
	}

	return selected
}

func waitForStableFile(path string, attempts int, delay time.Duration) error {
	var prevSize int64 = -1
	var lastNonZeroSize int64

	for i := 0; i < attempts; i++ {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("unable to stat file: %w", err)
		}

		size := info.Size()
		if size > 0 {
			lastNonZeroSize = size
		}

		if size == prevSize && size > 0 {
			if err := validateAudioFile(path); err != nil {
				return err
			}

			return nil
		}

		prevSize = size
		time.Sleep(delay)
	}

	if lastNonZeroSize == 0 {
		return fmt.Errorf("file remained empty after %d attempts", attempts)
	}

	return fmt.Errorf("file did not stabilize after %d attempts", attempts)
}

func validateAudioFile(path string) error {
	cmd := exec.Command("ffmpeg", "-v", "error", "-i", path, "-f", "null", "-")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("invalid audio file: %s", strings.TrimSpace(string(output)))
	}

	return nil
}

func dispatchMessage(message string, channel ChannelConfig, cfg Config) {
	platform := strings.ToLower(strings.TrimSpace(channel.Platform))
	if platform == "" {
		platform = "groupme"
	}

	switch platform {
	case "discord":
		sendDiscordMessage(message, channel, cfg)
	case "generic", "webhook":
		sendGenericWebhookMessage(message, channel, cfg)
	default:
		sendGroupMeMessage(message, channel, cfg)
	}
}

func chunkMessage(message string, maxLen int) []string {
	if maxLen <= 0 {
		maxLen = 1000
	}

	chunks := make([]string, 0, (len(message)/maxLen)+1)
	for start := 0; start < len(message); start += maxLen {
		end := start + maxLen
		if end > len(message) {
			end = len(message)
		}
		chunks = append(chunks, message[start:end])
	}

	return chunks
}

func sendGroupMeMessage(message string, channel ChannelConfig, cfg Config) {
	webhook := firstNonEmpty(channel.WebhookURL, cfg.GroupmeWebhookURL, cfg.WebhookURL)
	if strings.TrimSpace(webhook) == "" || channel.BotID == "" {
		log.Println("Missing GroupMe configuration for channel; skipping dispatch.")
		return
	}

	maxLen := cfg.MaxMessageLength
	if maxLen <= 0 {
		maxLen = 1000
	}

	for _, chunk := range chunkMessage(message, maxLen) {
		payload := map[string]string{
			"bot_id": channel.BotID,
			"text":   chunk,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Println("Error marshaling payload:", err)
			continue
		}

		req, err := http.NewRequest("POST", webhook, bytes.NewReader(payloadBytes))
		if err != nil {
			log.Println("Error creating request:", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("Error sending message to GroupMe:", err)
			continue
		}
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
			log.Println("Message sent to GroupMe channel", channel.Name)
		} else {
			log.Printf("Failed to send message to GroupMe: %d - %s\n", resp.StatusCode, string(bodyBytes))
		}
	}
}

func sendDiscordMessage(message string, channel ChannelConfig, cfg Config) {
	webhook := firstNonEmpty(channel.WebhookURL, cfg.DiscordWebhookURL, cfg.GenericWebhookURL, cfg.WebhookURL)
	if strings.TrimSpace(webhook) == "" {
		log.Println("Missing Discord webhook for channel; skipping dispatch.")
		return
	}

	maxLen := cfg.MaxMessageLength
	if maxLen <= 0 || maxLen > 2000 {
		maxLen = 2000
	}

	for _, chunk := range chunkMessage(message, maxLen) {
		payload := map[string]string{
			"content": chunk,
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Println("Error marshaling Discord payload:", err)
			continue
		}

		req, err := http.NewRequest("POST", webhook, bytes.NewReader(payloadBytes))
		if err != nil {
			log.Println("Error creating Discord request:", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("Error sending message to Discord:", err)
			continue
		}
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Println("Message sent to Discord channel", channel.Name)
		} else {
			log.Printf("Failed to send message to Discord: %d - %s\n", resp.StatusCode, string(bodyBytes))
		}
	}
}

func sendGenericWebhookMessage(message string, channel ChannelConfig, cfg Config) {
	webhook := firstNonEmpty(channel.WebhookURL, cfg.GenericWebhookURL, cfg.WebhookURL)
	if strings.TrimSpace(webhook) == "" {
		log.Println("Missing generic webhook for channel; skipping dispatch.")
		return
	}

	maxLen := cfg.MaxMessageLength
	if maxLen <= 0 {
		maxLen = 1000
	}

	for _, chunk := range chunkMessage(message, maxLen) {
		payload := map[string]string{
			"text": chunk,
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Println("Error marshaling generic webhook payload:", err)
			continue
		}

		req, err := http.NewRequest("POST", webhook, bytes.NewReader(payloadBytes))
		if err != nil {
			log.Println("Error creating generic webhook request:", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("Error sending generic webhook message:", err)
			continue
		}
		bodyBytes, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Println("Message sent via generic webhook for channel", channel.Name)
		} else {
			log.Printf("Failed to send generic webhook: %d - %s\n", resp.StatusCode, string(bodyBytes))
		}
	}
}

func storeTranscription(fileName, originalText, correctedText, source string, metadata StreamMetadata) {
	t := Transcription{
		Timestamp:     time.Now(),
		FileName:      fileName,
		OriginalText:  originalText,
		CorrectedText: correctedText,
		Source:        strings.ToUpper(strings.TrimSpace(source)),
		Metadata:      metadata,
	}

	persistTranscription(t)

	transcriptionsMux.Lock()
	defer transcriptionsMux.Unlock()

	transcriptions = append(transcriptions, t)

	if len(transcriptions) > maxTranscriptions {
		transcriptions = transcriptions[len(transcriptions)-maxTranscriptions:]
	}
}

func startWebServer(port string) {
	http.HandleFunc("/", transcriptionsHandler)
	http.HandleFunc("/transcriptions", transcriptionsHandler)
	http.HandleFunc("/api/config", configAPIHandler)

	log.Printf("Starting web server on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal("Error starting web server:", err)
	}
}

func transcriptionsHandler(w http.ResponseWriter, r *http.Request) {
	transcriptionsMux.Lock()
	defer transcriptionsMux.Unlock()

	var lastUpdated time.Time
	if len(transcriptions) > 0 {
		lastUpdated = transcriptions[len(transcriptions)-1].Timestamp
	}

	cfg := getConfig()

	data := PageData{
		Transcriptions: transcriptions,
		Total:          len(transcriptions),
		LastUpdated:    lastUpdated,
		UploadDir:      cfg.UploadDir,
	}

	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Error rendering template", http.StatusInternalServerError)
		log.Println("Error executing template:", err)
	}
}

func configAPIHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		respondWithJSON(w, getConfig())
	case http.MethodPut:
		var incoming Config
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			http.Error(w, "Invalid configuration payload", http.StatusBadRequest)
			return
		}

		updated := mergeConfig(getConfig(), incoming)
		if err := validateConfig(updated); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		previous := getConfig()
		setConfig(updated)

		if err := saveConfig(updated); err != nil {
			http.Error(w, "Failed to persist configuration", http.StatusInternalServerError)
			return
		}

		if previous.UploadDir != updated.UploadDir || previous.WatchDir != updated.WatchDir {
			if err := os.MkdirAll(updated.UploadDir, 0o755); err != nil {
				http.Error(w, "Failed to prepare upload directory", http.StatusInternalServerError)
				return
			}
			if err := startUploadWatcher(updated.UploadDir); err != nil {
				http.Error(w, "Failed to restart upload watcher", http.StatusInternalServerError)
				return
			}
		}

		if previous.DatabasePath != updated.DatabasePath {
			if err := initDatabase(updated.DatabasePath); err != nil {
				http.Error(w, "Failed to switch database", http.StatusInternalServerError)
				return
			}
		}

		if previous.FTPEnabled != updated.FTPEnabled || previous.FTPPort != updated.FTPPort || previous.UploadDir != updated.UploadDir || previous.FTPUser != updated.FTPUser || previous.FTPPassword != updated.FTPPassword {
			if err := startFTPServer(updated); err != nil {
				http.Error(w, "Failed to restart FTP server", http.StatusInternalServerError)
				return
			}
		}

		respondWithJSON(w, updated)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func respondWithJSON(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

func mergeConfig(current, incoming Config) Config {
	if incoming.Environment != "" {
		current.Environment = incoming.Environment
	}
	if incoming.WatchDir != "" {
		current.WatchDir = incoming.WatchDir
		current.UploadDir = incoming.WatchDir
	}
	if incoming.UploadDir != "" {
		current.UploadDir = incoming.UploadDir
		current.WatchDir = incoming.UploadDir
	}
	if current.UploadDir == "" {
		current.UploadDir = current.WatchDir
	}
	if current.WatchDir == "" {
		current.WatchDir = current.UploadDir
	}
	if incoming.OpenAIAPIKey != "" {
		current.OpenAIAPIKey = incoming.OpenAIAPIKey
	}
	if incoming.GroupmeBotID != "" {
		current.GroupmeBotID = incoming.GroupmeBotID
	}
	if incoming.GroupmeWebhookURL != "" {
		current.GroupmeWebhookURL = incoming.GroupmeWebhookURL
	}
	if incoming.DiscordWebhookURL != "" {
		current.DiscordWebhookURL = incoming.DiscordWebhookURL
	}
	if incoming.GenericWebhookURL != "" {
		current.GenericWebhookURL = incoming.GenericWebhookURL
	}
	if incoming.WebhookURL != "" {
		current.WebhookURL = incoming.WebhookURL
	}
	current.SendUploadNotification = incoming.SendUploadNotification
	current.SendTranscription = incoming.SendTranscription
	current.IncludeAudioLink = incoming.IncludeAudioLink
	if incoming.ProcessingDelay > 0 {
		current.ProcessingDelay = incoming.ProcessingDelay
	}
	if incoming.MaxMessageLength > 0 {
		current.MaxMessageLength = incoming.MaxMessageLength
	}
	if incoming.WebServerPort != "" {
		current.WebServerPort = incoming.WebServerPort
	}
	if incoming.OpenAIModel != "" {
		current.OpenAIModel = incoming.OpenAIModel
	}
	if incoming.OpenAITranscription != "" {
		current.OpenAITranscription = incoming.OpenAITranscription
	}
	if incoming.GroupmeMessageSuffix != "" {
		current.GroupmeMessageSuffix = incoming.GroupmeMessageSuffix
	}
	if len(incoming.Channels) > 0 {
		current.Channels = incoming.Channels
	}
	if incoming.DatabasePath != "" {
		current.DatabasePath = incoming.DatabasePath
	}
	if incoming.PreferredNoiseFilter != "" {
		current.PreferredNoiseFilter = incoming.PreferredNoiseFilter
	}
	current.FTPEnabled = incoming.FTPEnabled
	if incoming.FTPPort != "" {
		current.FTPPort = incoming.FTPPort
	}
	if incoming.FTPUser != "" || incoming.FTPPassword != "" {
		current.FTPUser = incoming.FTPUser
		current.FTPPassword = incoming.FTPPassword
	}
	if strings.TrimSpace(current.FTPPort) == "" {
		current.FTPPort = "2121"
	}

	return current
}

func validateConfig(cfg Config) error {
	uploadDir := cfg.UploadDir
	if uploadDir == "" {
		uploadDir = cfg.WatchDir
	}
	if uploadDir == "" {
		return fmt.Errorf("upload directory is required")
	}
	if cfg.ProcessingDelay < 0 {
		return fmt.Errorf("processing delay cannot be negative")
	}
	if cfg.MaxMessageLength <= 0 {
		return fmt.Errorf("max message length must be positive")
	}
	if cfg.WebServerPort == "" {
		return fmt.Errorf("web server port is required")
	}
	if cfg.FTPEnabled && strings.TrimSpace(cfg.FTPPort) == "" {
		return fmt.Errorf("FTP port is required when FTP is enabled")
	}

	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
