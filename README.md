# smtc2web-dev

一个为 smtc2web 环境开发的命令行工具



## 安装

```bash
cargo install --git https://github.com/smtc2web/smtcweb-dev.git
```

## 帮助

```log
smtc2web 主题命令行 Web 预览服务器

Usage: smtc2web-dev.exe [OPTIONS] [PATH]

Arguments:
  [PATH]  主题目录（需包含 theme.toml） [default: .]

Options:
  -P, --port <PORT>
          监听端口 [default: 3031]
      --host <HOST>
          监听地址 [default: 127.0.0.1]
      --no-open
          不自动打开浏览器
      --vite
          强制使用 Vite 模式（等价于 --framework vite）
      --vite-port <VITE_PORT>
          底层框架 dev server 的内部端口 [default: 5173]
      --framework <FRAMEWORK>
          底层框架：auto / none / vite / next / nuxt / astro / svelte / webpack / vue-cli / angular / script [default: auto]
      --process-filter <PROCESS_FILTER>
          媒体进程过滤规则（默认读取 smtc2web 的 config.toml，再退回 "*"）
  -h, --help
          Print help
  -V, --version
          Print version
```