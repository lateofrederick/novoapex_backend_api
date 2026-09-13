package workers

import "context"

// MediaDownloader fetches WhatsApp media bytes by media id;
// *orchestrator.MediaDownloader satisfies it.
type MediaDownloader interface {
	Download(ctx context.Context, mediaID string) ([]byte, string, error)
}

// AudioTranscriber turns a voice note into text; *openai.Client satisfies it.
type AudioTranscriber interface {
	TranscribeAudio(ctx context.Context, audio []byte, mime string) (string, error)
}

// NewMediaProcessor adapts the WhatsApp media downloader and the Whisper
// transcriber to the orchestrator's MediaProcessor seam.
func NewMediaProcessor(dl MediaDownloader, tr AudioTranscriber) MediaProcessor {
	return mediaProcessor{dl: dl, tr: tr}
}

type mediaProcessor struct {
	dl MediaDownloader
	tr AudioTranscriber
}

func (m mediaProcessor) Download(ctx context.Context, mediaID string) ([]byte, error) {
	data, _, err := m.dl.Download(ctx, mediaID)
	return data, err
}

// Transcribe sends the buffer as audio.ogg (WhatsApp voice notes are
// ogg/opus), exactly like AudioTranscriptionService's toFile(buffer, 'audio.ogg').
func (m mediaProcessor) Transcribe(ctx context.Context, media []byte) (string, error) {
	return m.tr.TranscribeAudio(ctx, media, "")
}
