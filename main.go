package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
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
	StreamDir      string
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
	Environment           string          `json:"environment"`
	WatchDir              string          `json:"watchDir"`
	UploadDir             string          `json:"uploadDir"`
	StreamDir             string          `json:"streamDir"`
	OpenAIAPIKey          string          `json:"openaiApiKey"`
	GroupmeBotID          string          `json:"groupmeBotId"`
	GroupmeWebhookURL     string          `json:"groupmeWebhookUrl"`
	DiscordWebhookURL     string          `json:"discordWebhookUrl"`
	GenericWebhookURL     string          `json:"genericWebhookUrl"`
	WebhookURL            string          `json:"webhookUrl"`
	ProcessingDelay       int             `json:"processingDelay"`
	StreamProcessingDelay int             `json:"streamProcessingDelay"`
	MaxMessageLength      int             `json:"maxMessageLength"`
	WebServerPort         string          `json:"webServerPort"`
	OpenAIModel           string          `json:"openaiModel"`
	GroupmeMessageSuffix  string          `json:"groupmeMessageSuffix"`
	OpenAITranscription   string          `json:"openaiTranscriptionModel"`
	Channels              []ChannelConfig `json:"channels"`
	StreamURL             string          `json:"streamUrl"`
	StreamSegmentSeconds  int             `json:"streamSegmentSeconds"`
}

type StreamMetadata struct {
	RawTags       map[string]string
	SelectedTag   string
	NormalizedTag string
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
	streamWatcherCancel context.CancelFunc
	streamWatcherWG     sync.WaitGroup
	streamCancel        context.CancelFunc
	streamWG            sync.WaitGroup
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Println("Error loading .env file, using environment variables")
	}
}

func loadConfig() Config {
	cfg := Config{
		Environment:           "dev",
		WatchDir:              "./watched_directory",
		UploadDir:             "./watched_directory",
		StreamDir:             "./stream_segments",
		GroupmeWebhookURL:     "https://api.groupme.com/v3/bots/post",
		ProcessingDelay:       1,
		StreamProcessingDelay: 60,
		MaxMessageLength:      1000,
		WebServerPort:         "8080",
		OpenAIModel:           "gpt-4.1",
		GroupmeMessageSuffix:  " - https://calls.sussexcountyalerts.com/",
		OpenAITranscription:   "gpt-4o-mini-transcribe",
		StreamSegmentSeconds:  60,
	}

	if data, err := os.ReadFile(configFilePath); err == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Printf("Unable to parse %s: %v", configFilePath, err)
		}
	} else {
		cfg.Environment = firstNonEmpty(os.Getenv("ENVIRONMENT"), cfg.Environment)
		cfg.WatchDir = firstNonEmpty(os.Getenv("WATCH_DIR"), cfg.WatchDir)
		cfg.UploadDir = firstNonEmpty(os.Getenv("UPLOAD_DIR"), cfg.UploadDir)
		cfg.StreamDir = firstNonEmpty(os.Getenv("STREAM_DIR"), cfg.StreamDir)
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
		if val := os.Getenv("STREAM_PROCESSING_DELAY"); val != "" {
			if parsed, err := strconv.Atoi(val); err == nil {
				cfg.StreamProcessingDelay = parsed
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
	if cfg.ProcessingDelay <= 0 {
		cfg.ProcessingDelay = 1
	}
	if cfg.StreamProcessingDelay <= 0 {
		cfg.StreamProcessingDelay = 60
	}
	if cfg.MaxMessageLength <= 0 {
		cfg.MaxMessageLength = 1000
	}
	if cfg.StreamSegmentSeconds <= 0 {
		cfg.StreamSegmentSeconds = 60
	}
	if cfg.StreamDir == "" {
		cfg.StreamDir = filepath.Join(cfg.UploadDir, "stream_segments")
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

func startUploadWatcher(dir string) error {
	return startDirectoryWatcher(dir, &uploadWatcherCancel, &uploadWatcherWG, handleUploadFile)
}

func startStreamWatcher(dir string) error {
	return startDirectoryWatcher(dir, &streamWatcherCancel, &streamWatcherWG, handleStreamFile)
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

func startStreamRecorder(streamURL, outputDir string, segmentSeconds int) error {
	if streamCancel != nil {
		streamCancel()
		streamWG.Wait()
	}

	normalizedURL, err := normalizeStreamURL(streamURL)
	if err != nil {
		return fmt.Errorf("invalid stream URL: %w", err)
	}

	if strings.TrimSpace(normalizedURL) == "" {
		return nil
	}

	if segmentSeconds <= 0 {
		segmentSeconds = 60
	}

	ctx, cancel := context.WithCancel(context.Background())
	streamCancel = cancel

	streamWG.Add(1)
	go func(url string, outDir string, segment int) {
		defer streamWG.Done()
		for ctx.Err() == nil {
			outputPattern := filepath.Join(outDir, "stream-%Y%m%d-%H%M%S.mp3")
			args := buildStreamRecorderArgs(url, outputPattern, segment)
			cmd := exec.CommandContext(ctx, "ffmpeg", args...)
			output, err := cmd.CombinedOutput()
			if err != nil && ctx.Err() == nil {
				log.Printf("Stream recorder stopped: %v - %s", err, string(output))
				time.Sleep(3 * time.Second)
				continue
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(1 * time.Second)
		}
	}(normalizedURL, outputDir, segmentSeconds)

	log.Printf("Started stream recorder for %s with %ds segments", redactCredentials(normalizedURL), segmentSeconds)

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

func normalizeStreamURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", err
	}

	if parsed.Scheme == "" {
		parsed.Scheme = "http"
	}
	if strings.EqualFold(parsed.Scheme, "icecast") {
		parsed.Scheme = "http"
	}

	return parsed.String(), nil
}

func redactCredentials(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}

	if parsed.User != nil {
		parsed.User = url.User("***")
	}

	return parsed.String()
}

func main() {
	cfg := loadConfig()
	if err := saveConfig(cfg); err != nil {
		log.Printf("Unable to persist configuration: %v", err)
	}

	setConfig(cfg)

	if err := os.MkdirAll(cfg.UploadDir, 0o755); err != nil {
		log.Fatalf("Error creating upload watch directory: %v", err)
	}

	if err := os.MkdirAll(cfg.StreamDir, 0o755); err != nil {
		log.Fatalf("Error creating stream watch directory: %v", err)
	}

	if err := startUploadWatcher(cfg.UploadDir); err != nil {
		log.Fatalf("Error starting upload directory watcher: %v", err)
	}

	if filepath.Clean(cfg.StreamDir) != filepath.Clean(cfg.UploadDir) {
		if err := startStreamWatcher(cfg.StreamDir); err != nil {
			log.Fatalf("Error starting stream directory watcher: %v", err)
		}
	}

	if err := startStreamRecorder(cfg.StreamURL, cfg.StreamDir, cfg.StreamSegmentSeconds); err != nil {
		log.Fatalf("Error starting stream recorder: %v", err)
	}

	tmplPath := filepath.Join("templates", "transcriptions.html")
	var errTpl error
	tmpl, errTpl = template.ParseFiles(tmplPath)
	if errTpl != nil {
		log.Fatalf("Error parsing template %s: %v", tmplPath, errTpl)
	}

	startWebServer(cfg.WebServerPort)
}

func watchDirectory(ctx context.Context, dir string, handler func(string)) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("error creating file watcher: %w", err)
	}
	defer watcher.Close()

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					if strings.HasSuffix(strings.ToLower(event.Name), ".mp3") {
						handler(event.Name)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("Watcher error:", err)
			case <-ctx.Done():
				return
			}
		}
	}()

	if err = watcher.Add(dir); err != nil {
		return fmt.Errorf("error adding directory to watcher: %w", err)
	}
	log.Println("Started watching directory:", dir)

	<-ctx.Done()

	return nil
}

func handleUploadFile(filePath string) {
	cfg := getConfig()
	delay := cfg.ProcessingDelay
	if delay < 0 {
		delay = 0
	}

	processNewFile(filePath, delay, cfg, "upload")
}

func handleStreamFile(filePath string) {
	cfg := getConfig()
	delay := cfg.StreamProcessingDelay
	if delay <= 0 {
		delay = 60
	}

	processNewFile(filePath, delay, cfg, "stream")
}

func processNewFile(filePath string, delaySeconds int, cfg Config, source string) {
	processedFilesMux.Lock()
	if processedFiles[filePath] {
		processedFilesMux.Unlock()
		log.Println("File already processed:", filePath)
		return
	}
	processedFiles[filePath] = true
	processedFilesMux.Unlock()

	log.Printf("[%s] New MP3 file detected: %s", source, filePath)
	if delaySeconds > 0 {
		time.Sleep(time.Duration(delaySeconds) * time.Second)
	}

	if err := waitForStableFile(filePath, 5, 500*time.Millisecond); err != nil {
		log.Printf("Skipping %s: %v", filepath.Base(filePath), err)
		return
	}

	fileName := filepath.Base(filePath)
	metadata := StreamMetadata{}
	if source == "stream" {
		metadata = extractStreamMetadata(filePath)
	}

	silent, err := isAudioSilent(filePath)
	if err != nil {
		log.Printf("Silence check failed for %s: %v", fileName, err)
	}
	if err == nil && silent {
		log.Printf("Skipping %s because the stream segment is silent", fileName)
		return
	}

	notifyUpload(fileName, cfg, source, metadata)

	transcription, err := transcribeAudio(filePath)
	if err != nil {
		log.Println("Error during transcription:", err)
		transcription = fmt.Sprintf("Transcription error: %v", err)
	}

	storeTranscription(fileName, transcription, transcription, source, metadata)

	sendTranscriptionToChannels(transcription, fileName, cfg, source, metadata)
}

func notifyUpload(fileName string, cfg Config, source string, metadata StreamMetadata) {
	for _, channel := range cfg.Channels {
		if !channel.SendUploadNotification {
			continue
		}
		link := buildAudioLink(cfg, channel, fileName)
		message := formatMessageWithMetadata(fmt.Sprintf("New call uploaded: %s", fileName), source, metadata)
		if channel.IncludeAudioLink && link != "" {
			message = fmt.Sprintf("%s %s", message, link)
		}
		dispatchMessage(message, channel, cfg)
	}
}

func sendTranscriptionToChannels(transcription, fileName string, cfg Config, source string, metadata StreamMetadata) {
	for _, channel := range cfg.Channels {
		if !channel.SendTranscription {
			continue
		}
		link := ""
		if channel.IncludeAudioLink {
			link = buildAudioLink(cfg, channel, fileName)
		}

		messageText := formatMessageWithMetadata(transcription, source, metadata)
		if link != "" {
			messageText = fmt.Sprintf("%s %s", messageText, link)
		}

		dispatchMessage(messageText, channel, cfg)
	}
}

func buildAudioLink(cfg Config, channel ChannelConfig, fileName string) string {
	suffix := strings.TrimSpace(channel.MessageSuffix)
	if suffix == "" {
		suffix = strings.TrimSpace(cfg.GroupmeMessageSuffix)
	}
	if suffix == "" {
		return ""
	}

	return fmt.Sprintf("%s%s", suffix, fileName)
}

func extractStreamMetadata(filePath string) StreamMetadata {
	meta := StreamMetadata{RawTags: make(map[string]string)}

	cmd := exec.Command("ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", filePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Unable to read stream metadata for %s: %v", filepath.Base(filePath), err)
		return meta
	}

	var probe struct {
		Format struct {
			Tags map[string]string `json:"tags"`
		} `json:"format"`
	}

	if err := json.Unmarshal(output, &probe); err != nil {
		log.Printf("Unable to parse metadata JSON for %s: %v", filepath.Base(filePath), err)
		return meta
	}

	if probe.Format.Tags != nil {
		meta.RawTags = probe.Format.Tags
	}

	meta.SelectedTag = firstNonEmpty(
		probe.Format.Tags["StreamTitle"],
		probe.Format.Tags["streamtitle"],
		probe.Format.Tags["title"],
		probe.Format.Tags["TITLE"],
		probe.Format.Tags["artist"],
		probe.Format.Tags["ARTIST"],
	)
	meta.SelectedTag = strings.TrimSpace(meta.SelectedTag)
	meta.NormalizedTag = normalizeTransmissionLabel(meta.SelectedTag)

	return meta
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

	cmd := exec.Command("ffmpeg", "-y", "-i", inputFilePath, "-af", "highpass=f=300, lowpass=f=3400", outputFilePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffmpeg error: %s", string(output))
	}

	return outputFilePath, nil
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
	transcriptionsMux.Lock()
	defer transcriptionsMux.Unlock()

	t := Transcription{
		Timestamp:     time.Now(),
		FileName:      fileName,
		OriginalText:  originalText,
		CorrectedText: correctedText,
		Source:        strings.ToUpper(strings.TrimSpace(source)),
		Metadata:      metadata,
	}

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
		StreamDir:      cfg.StreamDir,
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

		if previous.StreamDir != updated.StreamDir {
			if err := os.MkdirAll(updated.StreamDir, 0o755); err != nil {
				http.Error(w, "Failed to prepare stream directory", http.StatusInternalServerError)
				return
			}
			if err := startStreamWatcher(updated.StreamDir); err != nil {
				http.Error(w, "Failed to restart stream watcher", http.StatusInternalServerError)
				return
			}
		}

		if previous.StreamURL != updated.StreamURL || previous.StreamSegmentSeconds != updated.StreamSegmentSeconds || previous.StreamDir != updated.StreamDir {
			if err := startStreamRecorder(updated.StreamURL, updated.StreamDir, updated.StreamSegmentSeconds); err != nil {
				http.Error(w, "Failed to restart stream recorder", http.StatusInternalServerError)
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
	if incoming.StreamURL != "" {
		current.StreamURL = incoming.StreamURL
	}
	if incoming.StreamSegmentSeconds > 0 {
		current.StreamSegmentSeconds = incoming.StreamSegmentSeconds
	}
	if incoming.StreamDir != "" {
		current.StreamDir = incoming.StreamDir
	}
	if incoming.StreamProcessingDelay > 0 {
		current.StreamProcessingDelay = incoming.StreamProcessingDelay
	}
	if current.StreamProcessingDelay <= 0 {
		current.StreamProcessingDelay = 60
	}
	if current.StreamDir == "" {
		current.StreamDir = filepath.Join(current.UploadDir, "stream_segments")
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
	streamDir := cfg.StreamDir
	if streamDir == "" {
		streamDir = filepath.Join(uploadDir, "stream_segments")
	}
	if cfg.ProcessingDelay < 0 {
		return fmt.Errorf("processing delay cannot be negative")
	}
	if cfg.StreamProcessingDelay < 0 {
		return fmt.Errorf("stream processing delay cannot be negative")
	}
	if cfg.MaxMessageLength <= 0 {
		return fmt.Errorf("max message length must be positive")
	}
	if cfg.WebServerPort == "" {
		return fmt.Errorf("web server port is required")
	}
	if strings.TrimSpace(cfg.StreamURL) != "" && cfg.StreamSegmentSeconds <= 0 {
		return fmt.Errorf("stream segment seconds must be positive when stream URL is set")
	}
	if filepath.Clean(uploadDir) == filepath.Clean(streamDir) {
		return fmt.Errorf("stream directory must be different from upload directory")
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
