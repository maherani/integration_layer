# لایه مستقل یکپارچه‌سازی اسناد و هوش مصنوعی (Standalone Integration Layer)
## پل ارتباطی سبک، مستقل و بدون دیسک میان سامانه آرشیو و پیام‌رسان هوش مصنوعی

**محل استقرار مخزن:** `/home/alborz/integration-layer`  
**فناوری‌های پایه:** Python 3.10+، پروتکل استاندارد AMQP 0-9-1، استریم بایت‌به‌بایت HTTP  
**سرویس لینوکسی:** `archive-integration-layer.service`  
**وضعیت:** عملیاتی، ایزوله، آزموده با ۱۹ آزمون خودکار (۱۰۰٪ قبولی)  

---

## ۱. فلسفه طراحی و نقش در معماری
لایه یکپارچه‌سازی (**Integration Layer**) به عنوان یک پل سبک و مستقل عمل می‌کند تا سامانه آرشیو سازمانی (`enterprise-archive-system`) و سامانه هوش مصنوعی (`ai-box`) بدون نیاز به دسترسی مستقیم به پایگاه‌های داده یا فایل‌سیستم‌های یکدیگر با هم تبادل داده نمایند. این لایه دو وظیفه حیاتی بر عهده دارد:
1. **انتشار تضمینی رویدادها (Outbox Publisher):** دریافت رخدادهای معلق اسناد از آرشیو و انتشار پایدار آن‌ها در صف‌های RabbitMQ با تأییدیه ناشر (`Publisher Confirm`).
2. **پراکسی استریمینگ فایل‌ها (Streaming File Proxy):** تحویل محتوای اسناد به هوش مصنوعی به صورت استریم بایت‌به‌بایت با مصرف حافظه ثابت $O(1)$ و بدون نوشتن حتی ۱ بایت روی هارددیسک.

---

## ۲. اجزای اصلی سیستم

### الف) ناشر رویدادهای Outbox (`publisher.py`):
- **مکانیسم استعلام (Polling):** هر ۲ ثانیه یک‌بار با فراخوانی امن `GET /api/v1/outbox/pending` رخدادهای در وضعیت `PENDING` یا `FAILED` را از آرشیو دریافت می‌کند.
- **انتشار تأییدشده در صف:** رخدادها با شناسه یکتای `event_id`، وضعیت ماندگار (`delivery_mode=2`) و پرچم `mandatory=True` به کارگزار RabbitMQ فرستاده می‌شوند.
- **تأییدیه قطعی (ACK Loop):** تنها پس از دریافت تأیید قطعی تحویل پیام از سوی RabbitMQ، ناشر متد `POST /api/v1/outbox/ack` را فراخوانی می‌کند تا رکورد در آرشیو به وضعیت `PUBLISHED` تغییر یابد.
- **مقاومت در برابر قطعی کارگزار:** در صورت قطع ارتباط با RabbitMQ، ناشر به صورت خودکار با زمان انتظار پلکانی (Backoff) برای اتصال مجدد تلاش می‌کند و پیام‌ها در دیتابیس آرشیو امن می‌مانند.

### ب) پراکسی استریمینگ فایل بدون دیسک (`file_proxy.py`):
- **پورت شنود:** پورت `8090` (`http://127.0.0.1:8090/files/{fileId}`).
- **امنیت دسترسی:** احراز هویت با سرآیند `Authorization: Bearer <PROXY_AUTH_TOKEN>`.
- **استریم زنده و حافظه ثابت:** فایل‌ها در بسته‌های ۶۴ کیلوبایتی (`STREAM_CHUNK_SIZE=65536`) مستقیماً از API امن آرشیو واکشی و به کانسومر هوش مصنوعی تحویل داده می‌شوند.
- **عدم وابستگی به دیسک (Zero-Disk):** هیچ فایلی روی دیسک لوکال یا کش موقت لایه میانی ذخیره نمی‌شود که امنیت و سرعت سیستم را تضمین می‌کند.

### ج) کلاینت مستقل پروتکل AMQP (`amqp_client.py`):
- یک کلاینت اختصاصی و بهینه‌سازی‌شده برای پروتکل استاندارد AMQP 0-9-1 بدون نیاز به وابستگی‌های سنگین خارجی.
- پشتیبانی کامل از قابلیت‌های پیشرفته `confirm.select` (Publisher Confirms) و `basic.return` جهت تشخیص بسته‌های بدون مقصد.

---

## ۳. توپولوژی پیام‌رسانی RabbitMQ
* **نام Exchange:** `archive.events` (نوع: `direct`, پایدار `durable=True`)
* **کلید مسیریابی (Routing Key):** `file.created`
* **صف پردازش مقصد:** `ai_box.file_events` (پایدار `durable=True`)

---

## ۴. مدیریت سرویس لینوکسی (systemd)
این لایه به عنوان سرویس خودکار سیستم‌عامل تحت نام `archive-integration-layer.service` پیکربندی شده است:

```bash
# بررسی وضعیت سرویس:
sudo systemctl status archive-integration-layer.service

# شروع یا راه‌اندازی مجدد سرویس:
sudo systemctl restart archive-integration-layer.service

# توقف سرویس:
sudo systemctl stop archive-integration-layer.service

# مشاهده لاگ‌های زنده:
journalctl -u archive-integration-layer.service -f
```

---

## ۵. اجرای دستی و تست محلی (CLI)
برای اهداف تست یا توسعه محلی، می‌توانید اسکریپت `server.py` را مستقیماً اجرا کنید:

```bash
cd /home/alborz/integration-layer

# اجرای هر دو جزء (پراکسی فایل + ناشر Outbox):
python3 server.py --mode all

# اجرای اختصاصی پراکسی استریم فایل:
python3 server.py --mode proxy

# اجرای اختصاصی ناشر رویدادهای Outbox:
python3 server.py --mode publisher
```

---

## ۶. متغیرهای پیکربندی و محیطی

| متغیر | مقدار پیش‌فرض | توضیحات |
| :--- | :--- | :--- |
| `ARCHIVE_API_BASE_URL` | `http://127.0.0.1:80/index.php/apps/archive_autotag` | آدرس پایه API نرم‌افزار آرشیو |
| `ARCHIVE_SERVICE_TOKEN` | `ai_service_token_secret` | توکن مجاز ماشینی برای دسترسی به اندپوینت‌های آرشیو |
| `RABBITMQ_HOST` | `127.0.0.1` | آدرس هاست کارگزار پیام RabbitMQ |
| `RABBITMQ_PORT` | `5672` | پورت AMQP کارگزار RabbitMQ |
| `RABBITMQ_USER` | `guest` | نام کاربری کارگزار RabbitMQ |
| `RABBITMQ_PASSWORD` | `guest` | کلمه عبور کارگزار RabbitMQ |
| `RABBITMQ_EXCHANGE` | `archive.events` | نام کارگزار مبادله رویدادها |
| `RABBITMQ_ROUTING_KEY` | `file.created` | کلید مسیر رویدادهای فایل |
| `PROXY_HOST` | `0.0.0.0` | آدرس گوش دادن پراکسی فایل |
| `PROXY_PORT` | `8090` | پورت پراکسی استریمینگ فایل |
| `PROXY_AUTH_TOKEN` | `ai_box_proxy_secret_token` | توکن مجاز هوش مصنوعی برای واکشی فایل از پراکسی |
| `STREAM_CHUNK_SIZE` | `65536` | حجم هر بسته استریمینگ بر حسب بایت (۶۴ کیلوبایت) |
| `POLL_INTERVAL_SECONDS` | `2.0` | فاصله زمانی بین بررسی‌های دوره‌ای Outbox |

---

## ۷. آزمون‌های خودکار و تضمین کیفیت (Unit & Integration Tests)
مجموعه آزمون‌های جامع شامل ۱۹ مورد تست شبیه‌سازی قطعی شبکه، نقض تأییدیه، نشت حافظه و خطاهای مسیریابی:

```bash
cd /home/alborz/integration-layer
PYTHONPATH=. python3 tests/run_all.py
```
تمام ۱۹ آزمون با موفقیت ۱۰۰٪ پاس می‌شوند.
