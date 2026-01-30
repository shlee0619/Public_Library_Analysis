# 도서관 데이터 전처리 및 정제 코드

import pandas as pd
import numpy as np
import os
import re

def clean_library_data(input_path, output_path):
    """
    도서관 데이터를 전처리하고 정제하는 함수
    

    input_path (str): 입력 CSV 파일 경로
    output_path (str): 출력 CSV 파일 경로
    """
    print(f"도서관 데이터 파일 '{input_path}' 로드 중...")
    
    try:
        # 인코딩 자동 감지 시도
        encodings = ['cp949', 'euc-kr', 'utf-8', 'utf-8-sig']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(input_path, encoding=encoding)
                print(f"인코딩 '{encoding}'으로 파일 로드 성공")
                break
            except UnicodeDecodeError:
                continue
        else:
            print("지원되는 인코딩으로 파일을 로드할 수 없습니다.")
            return False
    except Exception as e:
        print(f"파일 로드 중 오류 발생: {e}")
        return False
    
    print(f"원본 데이터: {df.shape[0]}행 x {df.shape[1]}열")
    
    # 필수 열 확인
    required_columns = ['도서관명']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"필수 열이 누락되었습니다: {missing_columns}")
        return False
    
    # 데이터 정제
    print("데이터 정제 중...")
    
    # 1. 중복 행 제거
    df_cleaned = df.drop_duplicates()
    print(f"중복 제거 후: {df_cleaned.shape[0]}행 (중복 {df.shape[0] - df_cleaned.shape[0]}개 제거)")
    
    # 2. 도서관명 정제
    if '도서관명' in df_cleaned.columns:
        # 앞뒤 공백 제거
        df_cleaned['도서관명'] = df_cleaned['도서관명'].str.strip()
        
        # 결측치 확인 및 제거
        missing_names = df_cleaned['도서관명'].isna().sum()
        if missing_names > 0:
            print(f"도서관명 결측치 {missing_names}개 제거")
            df_cleaned = df_cleaned.dropna(subset=['도서관명'])
    
    # 3. 수치형 데이터 정제
    numeric_columns = ['건물면적', '좌석수', '대출가능권수']
    for col in numeric_columns:
        if col in df_cleaned.columns:
            # 문자열을 숫자로 변환 (오류 발생 시 NaN으로 처리)
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
            
            # 음수 값을 NaN으로 처리
            negative_count = (df_cleaned[col] < 0).sum()
            if negative_count > 0:
                print(f"{col} 열에서 음수 값 {negative_count}개를 NaN으로 변경")
                df_cleaned.loc[df_cleaned[col] < 0, col] = np.nan
            
            # 이상치 처리 (상위 1% 초과 값을 상위 1% 값으로 대체)
            upper_limit = df_cleaned[col].quantile(0.99)
            outlier_count = (df_cleaned[col] > upper_limit).sum()
            if outlier_count > 0:
                print(f"{col} 열에서 이상치 {outlier_count}개 처리 (상한값: {upper_limit})")
                df_cleaned.loc[df_cleaned[col] > upper_limit, col] = upper_limit
    
    # 4. 주소 정보 정제
    address_columns = ['소재지도로명주소', '소재지지번주소']
    for col in address_columns:
        if col in df_cleaned.columns:
            # 앞뒤 공백 제거
            df_cleaned[col] = df_cleaned[col].str.strip()
    
    # 5. 시도명, 시군구명 추출 (소재지도로명주소에서)
    if '소재지도로명주소' in df_cleaned.columns:
        # 시도 추출 패턴
        sido_pattern = r'^(서울특별시|부산광역시|대구광역시|인천광역시|광주광역시|대전광역시|울산광역시|세종특별자치시|경기도|강원도|충청북도|충청남도|전라북도|전라남도|경상북도|경상남도|제주특별자치도)'
        
        # 시도명 추출
        df_cleaned['시도명'] = df_cleaned['소재지도로명주소'].str.extract(sido_pattern)
        
        # 시군구명 추출 (시도명 다음에 나오는 시/군/구)
        sigungu_pattern = r'^(?:서울특별시|부산광역시|대구광역시|인천광역시|광주광역시|대전광역시|울산광역시|세종특별자치시|경기도|강원도|충청북도|충청남도|전라북도|전라남도|경상북도|경상남도|제주특별자치도)\s+([^\s]+)'
        df_cleaned['시군구명'] = df_cleaned['소재지도로명주소'].str.extract(sigungu_pattern)
        
        print(f"시도명 추출: {df_cleaned['시도명'].nunique()}개 시도")
        print(f"시군구명 추출: {df_cleaned['시군구명'].nunique()}개 시군구")
    
    # 6. 결측치 처리
    for col in df_cleaned.columns:
        missing_count = df_cleaned[col].isna().sum()
        if missing_count > 0:
            print(f"{col} 열: 결측치 {missing_count}개 ({missing_count/len(df_cleaned)*100:.1f}%)")
    
    # 7. 최종 데이터 저장
    df_cleaned.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"정제된 데이터 {output_path}에 저장 완료: {df_cleaned.shape[0]}행 x {df_cleaned.shape[1]}열")
    
    return True

if __name__ == "__main__":
    # 파일 경로 설정
    input_path = '전국도서관표준데이터.csv'
    output_path = 'library_info_cleaned.csv'
    
    # 데이터 정제 실행
    clean_library_data(input_path, output_path)
