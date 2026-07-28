package akwadb

import (
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// S3Archiver ships gzip-compressed segments to any S3-compatible object store
// (AWS S3, MinIO, Cloudflare R2, ...) using AWS Signature Version 4.
type S3Archiver struct {
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	PathStyle bool
	UseTLS    bool
	Client    *http.Client
}

func (a *S3Archiver) ArchiveSegment(path string) error {
	if a.Endpoint == "" || a.Bucket == "" {
		return fmt.Errorf("s3 archiver: endpoint and bucket are required")
	}

	objKey := filepath.Base(path) + ".gz"
	if prefix := strings.Trim(a.Prefix, "/"); prefix != "" {
		objKey = prefix + "/" + objKey
	}

	tmp, err := os.CreateTemp("", "akwadb-archive-*.gz")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	defer tmp.Close()

	src, err := os.Open(path)
	if err != nil {
		return err
	}

	hasher := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(tmp, hasher))
	if _, err := io.Copy(gz, src); err != nil {
		gz.Close()
		src.Close()
		return err
	}
	src.Close()
	if err := gz.Close(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	stat, err := tmp.Stat()
	if err != nil {
		return err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	return a.put(objKey, tmp, stat.Size(), hex.EncodeToString(hasher.Sum(nil)))
}

func (a *S3Archiver) put(key string, body io.Reader, size int64, payloadHash string) error {
	scheme := "http"
	if a.UseTLS {
		scheme = "https"
	}
	endpoint := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(a.Endpoint, "https://"), "http://"), "/")

	host := endpoint
	encodedPath := "/" + s3URIEncode(key, false)
	if !a.PathStyle {
		host = a.Bucket + "." + endpoint
	} else {
		encodedPath = "/" + a.Bucket + encodedPath
	}

	region := a.Region
	if region == "" {
		region = "us-east-1"
	}

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{
		http.MethodPut, encodedPath, "", canonicalHeaders, signedHeaders, payloadHash,
	}, "\n")

	scope := dateStamp + "/" + region + "/s3/aws4_request"
	requestHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(requestHash[:]),
	}, "\n")

	signingKey := deriveSigningKey(a.SecretKey, dateStamp, region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	req, err := http.NewRequest(http.MethodPut, scheme+"://"+host+encodedPath, body)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Host = host
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("Content-Type", "application/gzip")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+a.AccessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)

	client := a.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("s3 put %s: status %d: %s", key, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func s3URIEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
