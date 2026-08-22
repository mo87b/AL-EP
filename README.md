# 🚀 Anime Sync & Downloader (GitHub Actions Worker)

سيرفر تشغيل آلي مستقل يعمل على **GitHub Actions** لجلب جدول الحلقات وتنزيلها عبر `aria2c` ورفعها فوراً إلى `Pixeldrain` وتحديث قاعدة بيانات `Turso`.

---

## ⚙️ خطوات الإعداد والتشغيل على GitHub:

### 1️⃣ إنشاء مستودع جديد على GitHub (New Repository):
1. افتح حسابك على [GitHub.com](https://github.com).
2. أنشئ مستودعاً جديداً (Public للحصول على دقائق غير محدودة مجاناً، أو Private).
3. ارفع محتويات هذا المجلد (`github-actions-worker`) إلى المستودع.

---

### 2️⃣ إضافة المفاتيح السرية (Repository Secrets):
داخل صفحة المستودع على GitHub:
1. اذهب إلى **Settings** ⬅️ **Secrets and variables** ⬅️ **Actions**.
2. اضغط على **New repository secret** وأضف المتغيرات التالية:

| Secret Name | القيمة |
| :--- | :--- |
| `TURSO_URL` | `https://arabic-cache-hibbv7.aws-eu-west-1.turso.io` |
| `TURSO_TOKEN` | التوكن الخاص بـ Turso DB |
| `PIXELDRAIN_API_KEY` | `3b08e5e8-f7d9-4827-8025-a13ea596541f` |
| `GAS_PROXY_URL` | رابط Google Apps Script Proxy |

---

### 3️⃣ التشغيل التلقائي والمتابعة:
* يعمل السيرفر تلقائياً **كل 15 دقيقة**.
* يمكنك أيضاً تشغيله يدوياً في أي لحظة بالدخول إلى تبويب **Actions** ثم اختيار **Anime Streaming Sync & Downloader** والضغط على **Run workflow**.
* يمكنك فتح أي تشغيل وقراءة الـ Logs وسرعة التحميل والرفع بشكل مباشر.
