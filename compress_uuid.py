import uuid
import base64


def compress_uuid_str(uuid_str: str) -> str:
    """
    문자열 형태의 UUID를 20자 Base85 문자열로 변환.
    """
    # 1) 문자열 → UUID 객체 생성
    u = uuid.UUID(uuid_str)

    # 2) UUID 객체 → 16바이트로 추출
    raw_bytes = u.bytes

    # 3) Base85 인코딩 (16바이트 → 20문자)
    compressed_bytes = base64.b85encode(raw_bytes)

    # 4) 최종 문자열로 디코딩
    return compressed_bytes.decode('ascii')

print(compress_uuid_str("5033af38-ced6-4943-b533-07aec1834170"))
