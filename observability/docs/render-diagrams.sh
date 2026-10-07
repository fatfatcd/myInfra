#!/usr/bin/env bash
# 从架构文档中抽取 mermaid 代码块，渲染为 SVG / PNG。
# 用法：./render-diagrams.sh [markdown 文件]
#
# 渲染引擎（按优先级）：
#   1. docker 镜像 minlag/mermaid-cli（自带 Chromium，无需系统库），离线环境可从 Harbor 拉取：
#        MMDC_IMAGE=harbor.example.local/tools/mermaid-cli:11.12.0 ./render-diagrams.sh
#   2. 本机 mmdc（RENDERER=local），需 Node.js 20+、npm i @mermaid-js/mermaid-cli@11.12.0，
#      且系统已装 Chromium 依赖库（libnss3、libatk 等）
#
# 中文字体：设置 FONT_DIR 指向含 Noto Sans SC / 思源黑体的目录，会挂载进容器；否则中文显示为方框。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOC="$(realpath "${1:-$SCRIPT_DIR/otel-observability-architecture.md}")"
OUT_DIR="$SCRIPT_DIR/diagrams"
MMDC_IMAGE="${MMDC_IMAGE:-minlag/mermaid-cli:11.12.0}"
MMDC_BIN="${MMDC_BIN:-mmdc}"
FONT_DIR="${FONT_DIR:-$HOME/.local/share/fonts/noto-cjk}"
TIMEOUT="${TIMEOUT:-90}"

if [ -z "${RENDERER:-}" ]; then
  if command -v docker >/dev/null && docker image inspect "$MMDC_IMAGE" >/dev/null 2>&1; then
    RENDERER=docker
  else
    RENDERER=local
  fi
fi

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/*.mmd "$OUT_DIR"/*.svg "$OUT_DIR"/*.png "$OUT_DIR"/*.err

# 按出现顺序抽取 mermaid 代码块，文件名使用所在章节标题
node - "$DOC" "$OUT_DIR" <<'EOF'
const fs = require('fs');
const path = require('path');
const [doc, outDir] = process.argv.slice(2);
const lines = fs.readFileSync(doc, 'utf8').split('\n');
let heading = 'diagram', inBlock = false, buf = [], n = 0;
for (const line of lines) {
  const h = line.match(/^#{2,4}\s+(.*)$/);
  if (!inBlock && h) heading = h[1];
  if (!inBlock && line.trim() === '```mermaid') { inBlock = true; buf = []; continue; }
  if (inBlock && line.trim() === '```') {
    inBlock = false; n++;
    const slug = heading.replace(/[\s/：:（）()+，,.]+/g, '-').replace(/^-+|-+$/g, '');
    const name = `${String(n).padStart(2, '0')}-${slug}.mmd`;
    fs.writeFileSync(path.join(outDir, name), buf.join('\n') + '\n');
    continue;
  }
  if (inBlock) buf.push(line);
}
console.log(`extracted ${n} diagrams`);
EOF

cat > "$OUT_DIR/.mermaid-config.json" <<'EOF'
{ "theme": "default", "themeVariables": { "fontFamily": "Noto Sans SC, Noto Sans CJK SC, Source Han Sans SC, Microsoft YaHei, sans-serif" } }
EOF
cat > "$OUT_DIR/.puppeteer.json" <<'EOF'
{ "args": ["--no-sandbox", "--disable-gpu"] }
EOF

# render <输入文件名> <输出文件名>，路径相对 OUT_DIR
render() {
  if [ "$RENDERER" = docker ]; then
    local font_mount=()
    [ -d "$FONT_DIR" ] && font_mount=(-v "$FONT_DIR":/usr/share/fonts/custom-cjk:ro)
    timeout "$TIMEOUT" docker run --rm -u "$(id -u):$(id -g)" \
      -v "$OUT_DIR":/data "${font_mount[@]}" "$MMDC_IMAGE" \
      -q -c /data/.mermaid-config.json -b white -s 2 -i "/data/$1" -o "/data/$2"
  else
    (cd "$OUT_DIR" && timeout "$TIMEOUT" "$MMDC_BIN" -q -p .puppeteer.json \
      -c .mermaid-config.json -b white -s 2 -i "$1" -o "$2")
  fi
}

echo "renderer: $RENDERER"
failed=()
for f in "$OUT_DIR"/*.mmd; do
  name="$(basename "$f" .mmd)"
  for fmt in svg png; do
    if ! render "$name.mmd" "$name.$fmt" >/dev/null 2>"$OUT_DIR/$name.err"; then
      failed+=("$name"); break
    fi
  done
  [ -s "$OUT_DIR/$name.err" ] || rm -f "$OUT_DIR/$name.err"
done

if [ "${#failed[@]}" -gt 0 ]; then
  for n in "${failed[@]}"; do echo "FAIL: $n"; sed -n '1,15p' "$OUT_DIR/$n.err"; echo; done
  exit 1
fi
rm -f "$OUT_DIR"/*.err
echo "rendered $(ls "$OUT_DIR"/*.svg | wc -l) diagrams to $OUT_DIR"
