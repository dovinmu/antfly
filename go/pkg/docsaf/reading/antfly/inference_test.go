package antfly

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/antflydb/antfly/go/pkg/docsaf/reading"
)

func TestEmptyOCRIsSuccessfulReading(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if calls > 1 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, `{"error":"fallback must not replace a valid blank result"}`)
			return
		}
		fmt.Fprint(w, `{"model":"native-reader","object":"list","data":[{"index":0,"object":"read","text":""}]}`)
	}))
	defer server.Close()
	reader, err := NewAntflyReadReader(AntflyConfig{BaseURL: server.URL, Models: []string{"native-reader", "fallback"}})
	if err != nil {
		t.Fatal(err)
	}
	results, err := reader.ReadDetailed(context.Background(), []reading.BinaryContent{{MIMEType: "image/png", Data: []byte("image")}}, nil)
	if err != nil {
		t.Fatalf("valid empty OCR was treated as failure: %v", err)
	}
	if len(results) != 1 || results[0].Text != "" || results[0].Model != "native-reader" {
		t.Fatalf("blank OCR lost its successful model attribution: %+v", results)
	}
}

func TestTruncatedVisionIsNotAcceptedAsExtraction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"model":"native-vision","choices":[{"index":0,"message":{"role":"assistant","content":"The photographed form has"},"finish_reason":"length"}]}`)
	}))
	defer server.Close()
	reader, err := NewAntflyGenerateReader(AntflyConfig{BaseURL: server.URL, Models: []string{"native-vision"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.ReadDetailed(context.Background(), []reading.BinaryContent{{MIMEType: "image/png", Data: []byte("image")}}, nil)
	if err == nil {
		t.Fatal("token-truncated description was accepted as a complete extraction")
	}
}

func TestEmptyGenerationRequiresExplicitTranscriptionSemantics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"model":"native-vision","choices":[{"index":0,"message":{"role":"assistant","content":""},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()
	page := []reading.BinaryContent{{MIMEType: "image/png", Data: []byte("image")}}

	descriptionReader, err := NewAntflyGenerateReader(AntflyConfig{BaseURL: server.URL, Models: []string{"native-vision"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := descriptionReader.ReadDetailed(context.Background(), page, nil); err == nil {
		t.Fatal("empty visual description was accepted")
	}

	transcriptionReader, err := NewAntflyGenerateReader(AntflyConfig{
		BaseURL: server.URL, Models: []string{"native-vision"}, AllowEmptyOutput: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	results, err := transcriptionReader.ReadDetailed(context.Background(), page, nil)
	if err != nil {
		t.Fatalf("explicitly valid empty transcription was rejected: %v", err)
	}
	if len(results) != 1 || results[0].Text != "" || results[0].Model != "native-vision" {
		t.Fatalf("empty transcription result = %+v", results)
	}
}
