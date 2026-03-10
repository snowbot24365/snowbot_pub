import uuid
import hashlib
import subprocess
import hashlib
import platform

def get_hardware_id():
    """
    [개선] 메인보드 UUID를 기반으로 고유 ID 생성 (Windows 전용)
    - MAC 주소 대신 변하지 않는 하드웨어 고유 값을 사용
    """
    try:
        # 1. 윈도우인지 확인
        if platform.system() == "Windows":
            # wmic 명령어로 메인보드 UUID 조회
            cmd = "wmic csproduct get uuid"
            uuid_output = subprocess.check_output(cmd, shell=True).decode().split('\n')[1].strip()
            
            # 만약 제조사가 UUID를 제대로 입력하지 않아 'FFFF...' 등이 나오는 경우 대비
            if not uuid_output or "FFFF" in uuid_output:
                # 대안: CPU ID 사용
                cmd = "wmic cpu get processorid"
                uuid_output = subprocess.check_output(cmd, shell=True).decode().split('\n')[1].strip()
        else:
            # 리눅스/맥의 경우 기존 방식 또는 다른 명령어 사용 (여기선 기존 방식 fallback)
            import uuid
            uuid_output = str(uuid.getnode())

        # 2. 해싱 (SHA-256)
        hash_obj = hashlib.sha256(uuid_output.encode())
        hash_digest = hash_obj.hexdigest().upper()
        
        # 3. 포맷팅 (AAAA-BBBB-CCCC-DDDD)
        short_hash = hash_digest[:16]
        formatted_id = '-'.join([short_hash[i:i+4] for i in range(0, len(short_hash), 4)])
        
        return formatted_id

    except Exception as e:
        # 실패 시 비상용으로 기존 로직 사용하거나 에러 ID 반환
        print(f"HW ID 생성 실패: {e}")
        return "UNKNOWN-DEVICE-ID"

if __name__ == "__main__":
    print(f"My Hardware ID: {get_hardware_id()}")