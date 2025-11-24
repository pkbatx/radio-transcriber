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
	"os"
	"os/exec"
	"path/filepath"
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
}

type PageData struct {
	Transcriptions []Transcription
	Total          int
	LastUpdated    time.Time
	WatchDir       string
}

type ChannelConfig struct {
	Name                   string `json:"name"`
	BotID                  string `json:"botId"`
	WebhookURL             string `json:"webhookUrl"`
	MessageSuffix          string `json:"messageSuffix"`
	SendUploadNotification bool   `json:"sendUploadNotification"`
	SendTranscription      bool   `json:"sendTranscription"`
	IncludeAudioLink       bool   `json:"includeAudioLink"`
}

type Config struct {
	Environment          string          `json:"environment"`
	WatchDir             string          `json:"watchDir"`
	OpenAIAPIKey         string          `json:"openaiApiKey"`
	GroupmeBotID         string          `json:"groupmeBotId"`
	WebhookURL           string          `json:"webhookUrl"`
	ProcessingDelay      int             `json:"processingDelay"`
	MaxMessageLength     int             `json:"maxMessageLength"`
	SystemPrompt         string          `json:"systemPrompt"`
	WebServerPort        string          `json:"webServerPort"`
	OpenAIModel          string          `json:"openaiModel"`
	GroupmeMessageSuffix string          `json:"groupmeMessageSuffix"`
	OpenAITranscription  string          `json:"openaiTranscriptionModel"`
	Channels             []ChannelConfig `json:"channels"`
	StreamURL            string          `json:"streamUrl"`
	StreamSegmentSeconds int             `json:"streamSegmentSeconds"`
}

const defaultSystemPrompt = `You are a highly specialized transcription assistant for public safety dispatch communications.
Your task is to transcribe emergency radio transmissions with absolute precision. Ensure all unit identifiers, codes, locations, and technical terms are accurately captured. Apply correct spelling, punctuation, and capitalization. Do not include any irrelevant content, such as external links (e.g., websites), promotional messages, or information that was not explicitly communicated in the radio transmission. Ensure the transcription strictly reflects the call content as spoken.`

var (
	configFilePath    = "config.json"
	config            Config
	configMux         sync.RWMutex
	processedFiles    = make(map[string]bool)
	processedFilesMux sync.Mutex
	tmpl              *template.Template
	transcriptions    []Transcription
	transcriptionsMux sync.Mutex
	maxTranscriptions = 100 // Adjust as needed
	watcherCancel     context.CancelFunc
	watcherWG         sync.WaitGroup
	streamCancel      context.CancelFunc
	streamWG          sync.WaitGroup
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Println("Error loading .env file, using environment variables")
	}
}

func loadConfig() Config {
	cfg := Config{
		Environment:          "dev",
		WatchDir:             "./watched_directory",
		WebhookURL:           "https://api.groupme.com/v3/bots/post",
		ProcessingDelay:      1,
		MaxMessageLength:     1000,
		SystemPrompt:         defaultSystemPrompt,
		WebServerPort:        "8080",
		OpenAIModel:          "gpt-4",
		GroupmeMessageSuffix: " - https://calls.sussexcountyalerts.com/",
		OpenAITranscription:  "whisper-1",
		StreamSegmentSeconds: 60,
	}

	if data, err := os.ReadFile(configFilePath); err == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Printf("Unable to parse %s: %v", configFilePath, err)
		}
	} else {
		cfg.Environment = firstNonEmpty(os.Getenv("ENVIRONMENT"), cfg.Environment)
		cfg.WatchDir = firstNonEmpty(os.Getenv("WATCH_DIR"), cfg.WatchDir)
		cfg.OpenAIAPIKey = firstNonEmpty(os.Getenv("OPENAI_API_KEY"), cfg.OpenAIAPIKey)
		cfg.GroupmeBotID = firstNonEmpty(os.Getenv("GROUPME_BOT_ID"), cfg.GroupmeBotID)
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
		cfg.SystemPrompt = firstNonEmpty(os.Getenv("SYSTEM_PROMPT"), cfg.SystemPrompt)
		cfg.WebServerPort = firstNonEmpty(os.Getenv("WEB_SERVER_PORT"), cfg.WebServerPort)
		cfg.OpenAIModel = firstNonEmpty(os.Getenv("OPENAI_MODEL"), cfg.OpenAIModel)
		cfg.GroupmeMessageSuffix = firstNonEmpty(os.Getenv("GROUPME_MESSAGE_SUFFIX"), cfg.GroupmeMessageSuffix)
	}

	if cfg.SystemPrompt == "" {
		cfg.SystemPrompt = defaultSystemPrompt
	}
	if cfg.WatchDir == "" {
		cfg.WatchDir = "./watched_directory"
	}
	if cfg.WebServerPort == "" {
		cfg.WebServerPort = "8080"
	}
	if cfg.WebhookURL == "" {
		cfg.WebhookURL = "https://api.groupme.com/v3/bots/post"
	}
	if cfg.OpenAIModel == "" {
		cfg.OpenAIModel = "gpt-4"
	}
	if cfg.OpenAITranscription == "" {
		cfg.OpenAITranscription = "whisper-1"
	}
	if cfg.GroupmeMessageSuffix == "" {
		cfg.GroupmeMessageSuffix = " - https://calls.sussexcountyalerts.com/"
	}
	if cfg.ProcessingDelay <= 0 {
		cfg.ProcessingDelay = 1
	}
	if cfg.MaxMessageLength <= 0 {
		cfg.MaxMessageLength = 1000
	}
	if cfg.StreamSegmentSeconds <= 0 {
		cfg.StreamSegmentSeconds = 60
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
				BotID:                  cfg.GroupmeBotID,
				WebhookURL:             cfg.WebhookURL,
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

func startDirectoryWatcher(dir string) error {
	if watcherCancel != nil {
		watcherCancel()
		watcherWG.Wait()
	}

	ctx, cancel := context.WithCancel(context.Background())
	watcherCancel = cancel

	watcherWG.Add(1)
	go func(path string) {
		defer watcherWG.Done()
		if err := watchDirectory(ctx, path); err != nil {
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

	if strings.TrimSpace(streamURL) == "" {
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
			cmd := exec.CommandContext(ctx, "ffmpeg", "-y", "-i", url, "-c", "copy", "-f", "segment", "-segment_time", strconv.Itoa(segment), "-reset_timestamps", "1", "-write_id3v2", "1", "-strftime", "1", outputPattern)
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
	}(streamURL, outputDir, segmentSeconds)

	log.Printf("Started stream recorder for %s with %ds segments", streamURL, segmentSeconds)

	return nil
}

func main() {
	cfg := loadConfig()
	if err := saveConfig(cfg); err != nil {
		log.Printf("Unable to persist configuration: %v", err)
	}

	setConfig(cfg)

	if err := os.MkdirAll(cfg.WatchDir, 0o755); err != nil {
		log.Fatalf("Error creating watch directory: %v", err)
	}

	if err := startDirectoryWatcher(cfg.WatchDir); err != nil {
		log.Fatalf("Error starting directory watcher: %v", err)
	}

	if err := startStreamRecorder(cfg.StreamURL, cfg.WatchDir, cfg.StreamSegmentSeconds); err != nil {
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

func watchDirectory(ctx context.Context, dir string) error {
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
						handleNewFile(event.Name)
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

func handleNewFile(filePath string) {
	cfg := getConfig()

	processedFilesMux.Lock()
	if processedFiles[filePath] {
		processedFilesMux.Unlock()
		log.Println("File already processed:", filePath)
		return
	}
	processedFiles[filePath] = true
	processedFilesMux.Unlock()

	log.Println("New MP3 file detected:", filePath)
	time.Sleep(time.Duration(cfg.ProcessingDelay) * time.Second)

	fileName := filepath.Base(filePath)
	notifyUpload(fileName, cfg)

	transcription, err := transcribeAudio(filePath)
	if err != nil {
		log.Println("Error during transcription:", err)
		transcription = fmt.Sprintf("Transcription error: %v", err)
	}

	storeTranscription(fileName, transcription, transcription)

	sendTranscriptionToChannels(transcription, fileName, cfg)
}

func notifyUpload(fileName string, cfg Config) {
	for _, channel := range cfg.Channels {
		if !channel.SendUploadNotification {
			continue
		}
		link := buildAudioLink(cfg, channel, fileName)
		message := fmt.Sprintf("New call uploaded: %s", fileName)
		if channel.IncludeAudioLink && link != "" {
			message = fmt.Sprintf("%s %s", message, link)
		}
		sendGroupMeMessage(message, channel, cfg)
	}
}

func sendTranscriptionToChannels(transcription, fileName string, cfg Config) {
	for _, channel := range cfg.Channels {
		if !channel.SendTranscription {
			continue
		}
		link := ""
		if channel.IncludeAudioLink {
			link = buildAudioLink(cfg, channel, fileName)
		}

		messageText := transcription
		if link != "" {
			messageText = fmt.Sprintf("%s %s", transcription, link)
		}

		sendGroupMeMessage(messageText, channel, cfg)
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
		model = "whisper-1"
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

func postProcessTranscription(transcription string) (string, error) {
	cfg := getConfig()

	if cfg.OpenAIAPIKey == "" {
		return "", fmt.Errorf("missing OpenAI API key")
	}

	apiURL := "https://api.openai.com/v1/chat/completions"

	payload := map[string]interface{}{
		"model": cfg.OpenAIModel,
		"messages": []map[string]string{
			{"role": "system", "content": cfg.SystemPrompt},
			{"role": "user", "content": transcription},
		},
		"temperature": 0.0,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+cfg.OpenAIAPIKey)
	req.Header.Set("Content-Type", "application/json")

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
		return "", fmt.Errorf("OpenAI Chat API error: %s", string(bodyBytes))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err = json.Unmarshal(bodyBytes, &result); err != nil {
		return "", fmt.Errorf("failed to parse JSON response: %w", err)
	}

	if len(result.Choices) == 0 || result.Choices[0].Message.Content == "" {
		return "", fmt.Errorf("no content in response")
	}

	correctedTranscription := strings.TrimSpace(result.Choices[0].Message.Content)

	log.Println("Post-processing completed.")

	return correctedTranscription, nil
}

func sendGroupMeMessage(message string, channel ChannelConfig, cfg Config) {
	webhook := channel.WebhookURL
	if webhook == "" {
		webhook = cfg.WebhookURL
	}
	if webhook == "" || channel.BotID == "" {
		log.Println("Missing GroupMe configuration for channel; skipping dispatch.")
		return
	}

	maxLen := cfg.MaxMessageLength
	if maxLen <= 0 {
		maxLen = 1000
	}

	numMessages := int(float64(len(message)) / float64(maxLen))
	if len(message)%maxLen != 0 {
		numMessages++
	}

	for i := 0; i < numMessages; i++ {
		start := i * maxLen
		end := start + maxLen
		if end > len(message) {
			end = len(message)
		}
		chunk := message[start:end]
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
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
			log.Println("Message sent to GroupMe channel", channel.Name)
		} else {
			bodyBytes, _ := io.ReadAll(resp.Body)
			log.Printf("Failed to send message to GroupMe: %d - %s\n", resp.StatusCode, string(bodyBytes))
		}
	}
}

func storeTranscription(fileName, originalText, correctedText string) {
	transcriptionsMux.Lock()
	defer transcriptionsMux.Unlock()

	t := Transcription{
		Timestamp:     time.Now(),
		FileName:      fileName,
		OriginalText:  originalText,
		CorrectedText: correctedText,
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
		WatchDir:       cfg.WatchDir,
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

		if previous.WatchDir != updated.WatchDir {
			if err := os.MkdirAll(updated.WatchDir, 0o755); err != nil {
				http.Error(w, "Failed to prepare watch directory", http.StatusInternalServerError)
				return
			}
			if err := startDirectoryWatcher(updated.WatchDir); err != nil {
				http.Error(w, "Failed to restart directory watcher", http.StatusInternalServerError)
				return
			}
		}

		if previous.StreamURL != updated.StreamURL || previous.StreamSegmentSeconds != updated.StreamSegmentSeconds || previous.WatchDir != updated.WatchDir {
			if err := startStreamRecorder(updated.StreamURL, updated.WatchDir, updated.StreamSegmentSeconds); err != nil {
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
	}
	if incoming.OpenAIAPIKey != "" {
		current.OpenAIAPIKey = incoming.OpenAIAPIKey
	}
	if incoming.GroupmeBotID != "" {
		current.GroupmeBotID = incoming.GroupmeBotID
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
	if incoming.SystemPrompt != "" {
		current.SystemPrompt = incoming.SystemPrompt
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

	return current
}

func validateConfig(cfg Config) error {
	if cfg.WatchDir == "" {
		return fmt.Errorf("watch directory is required")
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
	if strings.TrimSpace(cfg.StreamURL) != "" && cfg.StreamSegmentSeconds <= 0 {
		return fmt.Errorf("stream segment seconds must be positive when stream URL is set")
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
