# Запуск `app.py` для hybrid TradingView -> BingX

## 1) Підготовка сервера

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx
```

## 2) Створити папку проекту

```bash
mkdir -p ~/bingx-hybrid-bot
cd ~/bingx-hybrid-bot
```

Скопіюй у цю папку файли:
- `app.py`
- `requirements.txt`
- `.env` (на основі `.env.example`)

## 3) Створити і активувати віртуальне середовище

```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 4) Встановити бібліотеки

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## 5) Налаштувати `.env`

Приклад:

```env
BINGX_API_KEY=your_real_key
BINGX_SECRET=your_real_secret
WEBHOOK_SECRET=your_long_random_secret
ALLOWED_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
DEFAULT_SETTLE=USDT
DEFAULT_MARGIN_MODE=cross
DRY_RUN=true
PORT=8000
DEDUP_TTL_SEC=120
```

> На першому запуску залиш `DRY_RUN=true`.

## 6) Локальний запуск

```bash
cd ~/bingx-hybrid-bot
source .venv/bin/activate
python3 app.py
```

Або так:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

Перевірка:

```bash
curl http://127.0.0.1:8000/health
```

## 7) Тест webhook локально через curl

```bash
curl -X POST http://127.0.0.1:8000/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "token": "your_long_random_secret",
    "action": "FULL_TP_CLOSE",
    "symbol": "BTCUSDT",
    "tpPercent": 1.1,
    "timestamp": 1710000000000
  }'
```

## 8) Публічний доступ через Nginx

Створи файл:

```bash
sudo nano /etc/nginx/sites-available/bingx-hybrid-bot
```

Встав:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Увімкни сайт:

```bash
sudo ln -s /etc/nginx/sites-available/bingx-hybrid-bot /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 9) HTTPS через Let's Encrypt

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

Після цього webhook URL буде:

```text
https://your-domain.com/webhook
```

## 10) Systemd автозапуск

Створи файл:

```bash
sudo nano /etc/systemd/system/bingx-hybrid-bot.service
```

Встав:

```ini
[Unit]
Description=BingX Hybrid TradingView Webhook Bot
After=network.target

[Service]
User=root
WorkingDirectory=/root/bingx-hybrid-bot
EnvironmentFile=/root/bingx-hybrid-bot/.env
ExecStart=/root/bingx-hybrid-bot/.venv/bin/python /root/bingx-hybrid-bot/app.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Запуск:

```bash
sudo systemctl daemon-reload
sudo systemctl enable bingx-hybrid-bot
sudo systemctl start bingx-hybrid-bot
sudo systemctl status bingx-hybrid-bot
```

Логи:

```bash
journalctl -u bingx-hybrid-bot -f
```

## 11) TradingView alert

У Pine має приходити JSON із `token` у body. Наприклад:

```json
{
  "token": "your_long_random_secret",
  "action": "FULL_TP_CLOSE",
  "symbol": "BTCUSDT",
  "tpPercent": 1.1,
  "timestamp": 1710000000000
}
```

Для твоєї hybrid-версії підтримуються action:
- `FIRST_SHORT`
- `RESTART_SHORT`
- `DCA_SHORT`
- `FULL_TP_CLOSE`
- `SUBCOVER_CLOSE`

## 12) Безпечний порядок тестування

1. `DRY_RUN=true`
2. `curl` тест на `/health`
3. `curl` тест на `/webhook`
4. Перевірити логи
5. Підключити TradingView
6. Лише потім поставити `DRY_RUN=false`

## 13) Якщо webhook із TradingView не доходить

Перевір:
- домен відкривається ззовні
- використовується `https://.../webhook`
- Nginx запущений
- сертифікат активний
- у `.env` правильний `WEBHOOK_SECRET`
- в alert body є поле `token`
- у логах `journalctl -u bingx-hybrid-bot -f` видно вхідні запити
