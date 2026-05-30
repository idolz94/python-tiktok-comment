import asyncio
import json
import os
import re
import urllib.error
import urllib.request
from typing import Any, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

AI_PROVIDER = os.getenv("AI_PROVIDER", "mock").strip().lower()
AI_ENABLED = os.getenv("AI_ENABLED", "1").strip() == "1"
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

print("AI_PROVIDER:", AI_PROVIDER)
print("AI_ENABLED:", AI_ENABLED)
print("AI_MODEL:", AI_MODEL)
print("OPENAI_API_KEY:", "OK" if OPENAI_API_KEY else "MISSING")

SYSTEM_COMMENT_KEYWORDS = [
    "đã tham gia",
    "đã chia sẻ",
    "đã thích",
    "đã follow",
    "joined",
    "shared the live",
    "liked the live",
    "followed the host",
    "welcome to tiktok live",
]


BUY_KEYWORDS = [
    "chốt",
    "chot",
    "lấy",
    "lay",
    "mua",
    "đặt",
    "dat",
    "order",
    "em lấy",
    "cho em",
    "lấy em",
    "mình lấy",
    "m lấy",
    "shop cho",
    "ốt",
    "chốt đơn",
    "lên đơn",
    "len don",
    "mua 1",
    "lấy 1",
    "x1",
    "x2",
    "x3",
]


PRICE_KEYWORDS = [
    "giá",
    "gia",
    "bao nhiêu",
    "bao nhieu",
    "bao tiền",
    "bao tien",
    "bn",
    "nhiêu tiền",
    "nhiu tiền",
    "nhiu",
    "nhiêu vậy",
    "nhiu vậy",
    "mấy tiền",
    "may tien",
    "tiền vậy",
    "tien vay",
    "giá sao",
    "gia sao",
    "giá nhiêu",
    "xin giá",
    "ib giá",
    "inbox giá",
]


STOCK_KEYWORDS = [
    "còn không",
    "còn k",
    "còn ko",
    "còn không shop",
    "còn hàng",
    "còn mẫu",
    "còn màu",
    "còn size",
    "còn hong",
    "còn hông",
    "có không",
    "có ko",
    "có k",
    "còn bản",
    "còn loại",
    "hết chưa",
    "hết hàng",
]


SHIPPING_KEYWORDS = [
    "ship",
    "cod",
    "phí ship",
    "phi ship",
    "giao hàng",
    "giao hang",
    "vận chuyển",
    "van chuyen",
    "freeship",
    "free ship",
    "fs",
    "gửi về",
    "gui ve",
    "ship tỉnh",
    "ship tỉnh không",
    "ship hà nội",
    "ship tphcm",
]


CONTACT_KEYWORDS = [
    "sđt",
    "sdt",
    "số điện thoại",
    "so dien thoai",
    "địa chỉ",
    "dia chi",
    "inbox",
    "ib",
    "nhắn em",
    "nhan em",
    "check ib",
    "rep ib",
]


PRODUCT_KEYWORDS = [
    # thời trang
    "size",
    "sz",
    "màu",
    "mau",
    "đen",
    "trắng",
    "be",
    "hồng",
    "xanh",
    "đỏ",
    "vàng",
    "nâu",
    "kem",
    "tím",
    "cam",
    "ghi",
    "xám",
    "mã",
    "ma",
    "cái",
    "bộ",
    "set",
    "áo",
    "quần",
    "váy",
    "đầm",

    # điện thoại / đồ công nghệ
    "iphone",
    "ip",
    "ipad",
    "samsung",
    "flip",
    "fold",
    "lip",
    "bose",
    "airpod",
    "airpods",
    "loa",
    "tai nghe",
    "sạc",
    "sac",
    "pin",
    "cáp",
    "cap",
    "thẻ nhớ",
    "the nho",
    "khe thẻ nhớ",
    "cam the nho",
    "cắm thẻ nhớ",
    "bộ nhớ",
    "bo nho",
    "gb",
    "ram",
    "rom",
    "máy",
    "may",
    "zin",
    "new",
    "like new",
    "bảo hành",
    "bao hanh",
    "chính hãng",
    "chinh hang",
]


PRODUCT_QUESTION_KEYWORDS = [
    "dùng được không",
    "dung duoc khong",
    "xài được không",
    "xai duoc khong",
    "cắm được không",
    "cam duoc khong",
    "có khe",
    "co khe",
    "khe thẻ",
    "khe the",
    "hỗ trợ",
    "ho tro",
    "có hỗ trợ",
    "co ho tro",
    "bảo hành không",
    "bao hanh khong",
    "pin sao",
    "pin khỏe",
    "pin khoe",
    "mấy gb",
    "may gb",
    "có thẻ nhớ",
    "co the nho",
]


NEGATIVE_KEYWORDS = [
    "không mua",
    "ko mua",
    "khỏi",
    "đắt quá",
    "dat qua",
    "mắc quá",
    "mac qua",
    "xem thôi",
    "xem thoi",
]


SPAM_KEYWORDS = [
    "haha",
    "hihi",
    "kkk",
    "xinh quá",
    "đẹp quá",
    "dep qua",
    "lag",
    "hello",
    "hi shop",
    "tim tim",
    "thả tim",
    "tha tim",
    "chào shop",
    "chao shop",
]


COLORS = [
    "đen",
    "trắng",
    "be",
    "hồng",
    "xanh",
    "đỏ",
    "vàng",
    "nâu",
    "kem",
    "tím",
    "cam",
    "ghi",
    "xám",
]


SIZES = ["xs", "s", "m", "l", "xl", "xxl", "2xl", "3xl"]


def normalize_ai_text(text: str) -> str:
    return str(text or "").lower().replace("\n", " ").strip()


def is_system_comment(text: str) -> bool:
    raw = normalize_ai_text(text)
    return any(keyword in raw for keyword in SYSTEM_COMMENT_KEYWORDS)


def has_any(raw: str, keywords: list[str]) -> bool:
    return any(keyword in raw for keyword in keywords)


def clamp_score(score: int) -> int:
    return max(0, min(100, int(score)))


def get_priority_level(score: int) -> str:
    if score >= 75:
        return "high"
    if score >= 45:
        return "medium"
    if score >= 25:
        return "low"
    return "normal"


def extract_quantity(raw: str) -> Optional[int]:
    patterns = [
        r"(?:chốt|chot|lấy|lay|mua|đặt|dat|order|cho em|em lấy)\s*(\d+)",
        r"x\s*(\d+)",
        r"\b(\d+)\s*(?:cái|bộ|set|chiếc|sp|con|c)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            try:
                value = int(match.group(1))
                if value > 0 and value < 100:
                    return value
            except Exception:
                return None

    return None


def extract_phone(raw: str) -> Optional[str]:
    match = re.search(r"(?:0|\+84)[0-9\s\-.]{8,13}", raw)

    if not match:
        return None

    return re.sub(r"[\s\-.]", "", match.group(0))


def extract_color(raw: str) -> Optional[str]:
    for color in COLORS:
        if color in raw:
            return color

    return None


def extract_size(raw: str) -> Optional[str]:
    for size in SIZES:
        if re.search(rf"\bsize\s*{re.escape(size)}\b|\bsz\s*{re.escape(size)}\b|\b{re.escape(size)}\b", raw):
            return size.upper()

    return None


def extract_product_code(raw: str) -> Optional[str]:
    patterns = [
        r"\bmã\s*([a-z0-9]{1,8})\b",
        r"\bma\s*([a-z0-9]{1,8})\b",
        r"\b([0-9]{1,4})\s*(?:đen|trắng|be|hồng|xanh|đỏ|vàng|nâu|kem|tím|cam|ghi|xám)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            return str(match.group(1))

    return None


def extract_product_name(raw: str) -> Optional[str]:
    product_words = [
        "iphone",
        "ipad",
        "samsung",
        "flip",
        "fold",
        "bose",
        "airpod",
        "airpods",
        "loa",
        "tai nghe",
        "thẻ nhớ",
        "the nho",
        "áo",
        "quần",
        "váy",
        "đầm",
        "set",
        "bộ",
    ]

    for keyword in product_words:
        if keyword in raw:
            return keyword

    return None


def detect_intent(raw: str, score: int) -> str:
    if has_any(raw, BUY_KEYWORDS):
        return "buy"

    if has_any(raw, PRICE_KEYWORDS):
        return "ask_price"

    if has_any(raw, STOCK_KEYWORDS):
        return "ask_stock"

    if has_any(raw, SHIPPING_KEYWORDS):
        return "ask_shipping"

    if has_any(raw, PRODUCT_QUESTION_KEYWORDS):
        return "ask_product"

    if has_any(raw, CONTACT_KEYWORDS):
        return "contact"

    if score >= 45 and has_any(raw, PRODUCT_KEYWORDS):
        return "ask_product"

    return "normal"


def build_order_info(raw: str, old_info: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    old_info = old_info or {}

    return {
        "productCode": old_info.get("productCode") or extract_product_code(raw),
        "productName": old_info.get("productName") or extract_product_name(raw),
        "quantity": old_info.get("quantity") or extract_quantity(raw),
        "color": old_info.get("color") or extract_color(raw),
        "size": old_info.get("size") or extract_size(raw),
        "phone": old_info.get("phone") or extract_phone(raw),
    }


def build_missing_info(intent: str, order_info: Dict[str, Any], final_score: int) -> list[str]:
    if final_score < 45:
        return []

    missing = []

    if intent == "buy":
        if not order_info.get("productName") and not order_info.get("productCode"):
            missing.append("Tên sản phẩm")

        if not order_info.get("quantity"):
            missing.append("Số lượng")

        if not order_info.get("phone"):
            missing.append("Số điện thoại")

        missing.append("Địa chỉ")

    elif intent in ["ask_price", "ask_stock", "ask_product"]:
        if not order_info.get("productName") and not order_info.get("productCode"):
            missing.append("Tên sản phẩm")

    elif intent == "ask_shipping":
        missing.append("Địa chỉ")

    return list(dict.fromkeys(missing))


def analyze_comment_by_rule(text: str) -> Dict[str, Any]:
    raw = normalize_ai_text(text)

    if is_system_comment(raw):
        return {
            "ruleScore": 0,
            "aiScore": None,
            "finalScore": 0,
            "intent": "spam",
            "priorityLevel": "normal",
            "matchedReasons": ["Comment hệ thống"],
            "orderInfo": {},
            "missingInfo": [],
            "aiStatus": "none",
            "shouldUseAI": False,
            "aiReason": "Comment hệ thống, không cần xử lý.",
        }

    score = 0
    reasons: list[str] = []

    if has_any(raw, BUY_KEYWORDS):
        score += 60
        reasons.append("Có ý định mua/chốt đơn")

    if has_any(raw, PRICE_KEYWORDS):
        score += 55
        reasons.append("Hỏi giá")

    if has_any(raw, STOCK_KEYWORDS):
        score += 45
        reasons.append("Hỏi còn hàng/tồn kho")

    if has_any(raw, PRODUCT_QUESTION_KEYWORDS):
        score += 45
        reasons.append("Hỏi thông tin sản phẩm/tính năng")

    if has_any(raw, SHIPPING_KEYWORDS):
        score += 40
        reasons.append("Hỏi vận chuyển")

    if has_any(raw, CONTACT_KEYWORDS):
        score += 35
        reasons.append("Có tín hiệu inbox/liên hệ")

    if has_any(raw, PRODUCT_KEYWORDS):
        score += 25
        reasons.append("Có thông tin sản phẩm")

    if re.search(r"\b\d+\b|x\s*\d+", raw):
        score += 20
        reasons.append("Có số/mã hàng/số lượng")

    if extract_phone(raw):
        score += 45
        reasons.append("Có số điện thoại")

    if has_any(raw, NEGATIVE_KEYWORDS):
        score -= 45
        reasons.append("Có tín hiệu không mua")

    if has_any(raw, SPAM_KEYWORDS):
        score -= 25
        reasons.append("Có dấu hiệu comment thường")

    if len(raw) <= 2:
        score -= 30
        reasons.append("Comment quá ngắn")

    score = clamp_score(score)

    intent = detect_intent(raw, score)
    priority_level = get_priority_level(score)
    order_info = build_order_info(raw)

    # Mở rộng AI:
    # - Trước đây chỉ gọi AI khi 30 <= score < 75
    # - Bản mới gọi AI từ 10 đến 84 để nhiều comment mơ hồ được nâng ưu tiên hơn
    should_use_ai = (
        AI_ENABLED
        and not is_system_comment(raw)
        and intent != "spam"
        and 10 <= score < 85
    )

    return {
        "ruleScore": score,
        "aiScore": None,
        "finalScore": score,
        "intent": intent,
        "priorityLevel": priority_level,
        "matchedReasons": reasons,
        "orderInfo": order_info,
        "missingInfo": build_missing_info(intent, order_info, score),
        "aiStatus": "pending" if should_use_ai else "none",
        "shouldUseAI": should_use_ai,
        "aiReason": "Rule phát hiện: " + ", ".join(reasons) if reasons else "Chưa có tín hiệu mua hàng rõ ràng.",
    }


def merge_ai_result(comment: Dict[str, Any], ai_result: Dict[str, Any]) -> Dict[str, Any]:
    current_score = int(comment.get("finalScore") or 0)
    ai_score = int(ai_result.get("aiScore") or ai_result.get("finalScore") or current_score)
    final_score = clamp_score(max(current_score, ai_score))

    intent = ai_result.get("intent") or comment.get("intent") or "normal"

    order_info = {
        **(comment.get("orderInfo") or {}),
        **(ai_result.get("orderInfo") or {}),
    }

    matched_reasons = list(dict.fromkeys(
        list(comment.get("matchedReasons") or []) + list(ai_result.get("matchedReasons") or [])
    ))

    return {
        **comment,
        "aiScore": ai_score,
        "finalScore": final_score,
        "intent": intent,
        "priorityLevel": get_priority_level(final_score),
        "orderInfo": order_info,
        "matchedReasons": matched_reasons,
        "missingInfo": build_missing_info(intent, order_info, final_score),
        "aiStatus": "done",
        "aiReason": ai_result.get("aiReason") or "AI đã phân tích và cập nhật độ ưu tiên.",
    }


async def call_openai_for_comment(raw: str, comment: Dict[str, Any]) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("Thiếu OPENAI_API_KEY")

    prompt = f"""
Bạn là AI phân loại comment livestream bán hàng tiếng Việt.

Hãy phân tích comment và trả về JSON hợp lệ.

Comment: {raw}

Quy tắc:
- Nếu khách có ý định mua/chốt/lấy/order: intent = buy, score cao.
- Nếu hỏi giá: intent = ask_price, score từ 55-80.
- Nếu hỏi còn hàng/tính năng/sản phẩm: intent = ask_stock hoặc ask_product, score từ 50-75.
- Nếu hỏi ship/vận chuyển: intent = ask_shipping, score từ 50-70.
- Nếu comment bình thường không liên quan mua hàng: intent = normal, score thấp.
- Ưu tiên đẩy nhiều comment có khả năng tạo đơn vào priority.
- Không trả lời ngoài JSON.

Schema JSON:
{{
  "aiScore": number,
  "intent": "buy" | "ask_price" | "ask_stock" | "ask_shipping" | "ask_product" | "contact" | "normal" | "spam",
  "matchedReasons": string[],
  "orderInfo": {{
    "productCode": string | null,
    "productName": string | null,
    "quantity": number | null,
    "color": string | null,
    "size": string | null,
    "phone": string | null
  }},
  "aiReason": string
}}
""".strip()

    payload = {
        "model": AI_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "Bạn chỉ trả về JSON hợp lệ, không markdown.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    def _request() -> Dict[str, Any]:
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=12) as res:
            body = json.loads(res.read().decode("utf-8"))

        content = body["choices"][0]["message"]["content"]
        return json.loads(content)

    result = await asyncio.to_thread(_request)

    return {
        "aiScore": clamp_score(int(result.get("aiScore") or 0)),
        "finalScore": clamp_score(int(result.get("aiScore") or 0)),
        "intent": result.get("intent") or comment.get("intent") or "normal",
        "matchedReasons": result.get("matchedReasons") or [],
        "orderInfo": {
            **build_order_info(raw, comment.get("orderInfo") or {}),
            **(result.get("orderInfo") or {}),
        },
        "aiStatus": "done",
        "aiReason": result.get("aiReason") or "AI đã phân tích comment.",
    }


async def analyze_comment_by_mock_ai(comment: Dict[str, Any]) -> Dict[str, Any]:
    await asyncio.sleep(0.3)

    raw = normalize_ai_text(comment.get("text") or comment.get("comment") or "")
    current_score = int(comment.get("finalScore") or 0)

    ai_score = current_score
    intent = comment.get("intent") or "normal"
    reasons: list[str] = []

    if has_any(raw, BUY_KEYWORDS):
        ai_score = max(ai_score, 90)
        intent = "buy"
        reasons.append("AI mock: comment có ý định mua/chốt")

    elif extract_phone(raw):
        ai_score = max(ai_score, 88)
        intent = "contact"
        reasons.append("AI mock: khách để lại số điện thoại")

    elif has_any(raw, PRICE_KEYWORDS):
        ai_score = max(ai_score, 72)
        intent = "ask_price"
        reasons.append("AI mock: khách đang hỏi giá")

    elif has_any(raw, STOCK_KEYWORDS) or has_any(raw, PRODUCT_QUESTION_KEYWORDS):
        ai_score = max(ai_score, 68)
        intent = "ask_stock"
        reasons.append("AI mock: khách hỏi sản phẩm/tồn kho/tính năng")

    elif has_any(raw, SHIPPING_KEYWORDS):
        ai_score = max(ai_score, 62)
        intent = "ask_shipping"
        reasons.append("AI mock: khách hỏi vận chuyển")

    elif has_any(raw, PRODUCT_KEYWORDS) and re.search(r"\?", raw):
        ai_score = max(ai_score, 55)
        intent = "ask_product"
        reasons.append("AI mock: khách hỏi thông tin sản phẩm")

    elif has_any(raw, PRODUCT_KEYWORDS):
        ai_score = max(ai_score, 40)
        intent = "ask_product"
        reasons.append("AI mock: có nhắc tới sản phẩm")

    order_info = build_order_info(raw, comment.get("orderInfo") or {})

    return {
        "aiScore": clamp_score(ai_score),
        "finalScore": clamp_score(ai_score),
        "intent": intent,
        "priorityLevel": get_priority_level(ai_score),
        "orderInfo": order_info,
        "missingInfo": build_missing_info(intent, order_info, ai_score),
        "matchedReasons": reasons,
        "aiStatus": "done",
        "aiReason": reasons[0] if reasons else "AI mock chưa thấy tín hiệu mua hàng rõ hơn rule.",
    }


async def analyze_comment_by_ai(comment: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hàm này nên chạy background task, không chạy trực tiếp trong callback TikTokLive.

    Provider:
    - AI_PROVIDER=mock: AI giả lập, không tốn phí, dùng để test UI.
    - AI_PROVIDER=openai: gọi OpenAI nếu có OPENAI_API_KEY.
    """
    raw = normalize_ai_text(comment.get("text") or comment.get("comment") or "")

    if not AI_ENABLED:
        return {
            **comment,
            "aiStatus": "none",
            "aiReason": "AI đang tắt.",
        }

    try:
        if AI_PROVIDER == "openai":
            ai_result = await call_openai_for_comment(raw, comment)
        else:
            ai_result = await analyze_comment_by_mock_ai(comment)

        return merge_ai_result(comment, ai_result)

    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, RuntimeError, json.JSONDecodeError) as error:
        # Nếu AI lỗi thì không làm hỏng luồng live.
        # Vẫn trả comment rule ban đầu để frontend hiển thị.
        return {
            **comment,
            "aiStatus": "error",
            "aiReason": f"AI lỗi, dùng rule score: {str(error)}",
            "priorityLevel": get_priority_level(int(comment.get("finalScore") or 0)),
        }