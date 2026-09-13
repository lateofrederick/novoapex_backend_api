package apidocs

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"

	"github.com/go-chi/chi/v5"
	"gopkg.in/yaml.v3"
)

// swagger-ui-dist assets (Apache-2.0, see swaggerui/LICENSE), served locally
// like @nestjs/swagger does — no CDN dependency.
//
//go:embed swaggerui
var assets embed.FS

// Paths match SwaggerModule.setup('api-docs', app, document).
const (
	UIPath   = "/api-docs"
	JSONPath = "/api-docs-json"
	YAMLPath = "/api-docs-yaml"
)

// Enabled mirrors apps/api/src/main.ts: the docs are exposed outside
// production, and in production only when ENABLE_SWAGGER=true (e.g. staging).
func Enabled(nodeEnv string, enableSwagger bool) bool {
	return nodeEnv != "production" || enableSwagger
}

// Mount registers Swagger UI, the JSON and YAML documents, and the UI assets.
// These routes are public, like Nest's (served outside the controller guard).
func Mount(r chi.Router, opts Options) error {
	doc := Spec(opts)
	jsonDoc, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	// Round-trip through JSON so YAML key order and scalar types match.
	var generic any
	if err := json.Unmarshal(jsonDoc, &generic); err != nil {
		return err
	}
	yamlDoc, err := yaml.Marshal(generic)
	if err != nil {
		return err
	}
	ui, err := fs.Sub(assets, "swaggerui")
	if err != nil {
		return err
	}

	r.Get(JSONPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write(jsonDoc)
	})
	r.Get(YAMLPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
		_, _ = w.Write(yamlDoc)
	})
	index := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(indexHTML))
	}
	r.Get(UIPath, index)
	r.Get(UIPath+"/", index)
	r.Handle(UIPath+"/*", http.StripPrefix(UIPath+"/", http.FileServerFS(ui)))
	return nil
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>NovOApex API</title>
  <link rel="stylesheet" type="text/css" href="/api-docs/swagger-ui.css">
  <link rel="icon" type="image/png" href="/api-docs/favicon-32x32.png" sizes="32x32">
  <link rel="icon" type="image/png" href="/api-docs/favicon-16x16.png" sizes="16x16">
  <style>html{box-sizing:border-box;overflow-y:scroll}*,*:before,*:after{box-sizing:inherit}body{margin:0;background:#fafafa}</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="/api-docs/swagger-ui-bundle.js" charset="UTF-8"></script>
  <script src="/api-docs/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
  <script>
    window.onload = function () {
      window.ui = SwaggerUIBundle({
        url: "/api-docs-json",
        dom_id: "#swagger-ui",
        deepLinking: true,
        persistAuthorization: true,
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
        plugins: [SwaggerUIBundle.plugins.DownloadUrl],
        layout: "StandaloneLayout"
      });
    };
  </script>
</body>
</html>
`
