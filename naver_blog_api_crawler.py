import pandas as pd
import requests
import json
import time
import os
import csv
from urllib.parse import quote
from datetime import datetime

class NaverBlogSearchAPI:
    """네이버 블로그 검색 API를 활용한 도서관 리뷰 수집 클래스"""
    
    def __init__(self, client_id=None, client_secret=None):
        """클라이언트 ID와 시크릿을 초기화."""
       
        self.client_id = client_id or "MhwFqrTamD_qfZ24piL6"
        self.client_secret = client_secret or "lAQ4RNsifH"
        self.api_url = "https://openapi.naver.com/v1/search/blog.json"
        self.headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
            "Content-Type": "application/json"
        }
        
    def search_blog(self, query, display=10, start=1, sort="sim"):
        """블로그 검색을 실행하고 결과를 반환."""
        # 검색어 URL 인코딩
        encoded_query = quote(query)
        
        # 요청 URL 구성
        url = f"{self.api_url}?query={encoded_query}&display={display}&start={start}&sort={sort}"
        
        try:
            # API 요청
            response = requests.get(url, headers=self.headers)
            
            # 응답 상태 확인
            if response.status_code == 200:
                result = response.json()
                return result
            else:
                print(f"Error: API 요청 실패 (상태 코드: {response.status_code})")
                print(f"응답 내용: {response.text}")
                return None
                
        except Exception as e:
            print(f"예외 발생: {e}")
            return None
    
    def get_all_blog_reviews(self, library_name, max_reviews=10, delay=0.5):
        """
        특정 도서관에 대한 블로그 리뷰를 수집.
        """
        search_query = f"{library_name} 도서관"
        print(f"'{search_query}' 검색 중...")
        
        reviews = []
        start = 1
        display = min(100, max_reviews)  # 한 번에 최대 100개까지 요청 가능
        
        while len(reviews) < max_reviews:
            result = self.search_blog(search_query, display=display, start=start)
            
            if not result or 'items' not in result or not result['items']:
                print(f"더 이상 검색 결과가 없거나 오류가 발생했습니다.")
                break
                
            # 검색 결과 처리
            for item in result['items']:
                # HTML 태그 제거 및 텍스트 정리
                title = self._clean_text(item.get('title', ''))
                description = self._clean_text(item.get('description', ''))
                
                review_data = {
                    '도서관명': library_name,
                    '블로그제목': title,
                    '블로그내용': description,
                    '블로그링크': item.get('link', ''),
                    '블로그이름': item.get('bloggername', ''),
                    '포스팅날짜': item.get('postdate', '')
                }
                
                reviews.append(review_data)
                
                if len(reviews) >= max_reviews:
                    break
            
            # 다음 페이지로 이동
            start += display
            
            # API 호출 제한을 고려한 지연
            time.sleep(delay)
            
            # 최대 1000개까지만 조회 가능 (네이버 API 제한)
            if start > 1000:
                print(f"네이버 API 제한으로 인해 검색을 중단합니다 (최대 1000개).")
                break
        
        print(f"'{library_name}' 도서관에 대한 블로그 리뷰 {len(reviews)}개 수집 완료")
        return reviews
    
    def _clean_text(self, text):
        """HTML 태그를 제거하고 텍스트를 정리."""
        import re
        # HTML 태그 제거
        text = re.sub(r'<[^>]+>', '', text)
        # 특수문자 처리
        text = text.replace('&quot;', '"').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        return text.strip()


def collect_library_reviews(library_data_path, output_path, client_id=None, client_secret=None, 
                           reviews_per_library=5, delay_between_libraries=1):
    """도서관 데이터를 읽고 각 도서관에 대한 블로그 리뷰를 수집하여 CSV 파일로 저장."""
    # 도서관 데이터 로드
    try:
        library_df = pd.read_csv(library_data_path)
        if '도서관명' not in library_df.columns:
            print("오류: 도서관 데이터에 '도서관명' 열이 없습니다.")
            return
    except Exception as e:
        print(f"도서관 데이터 로드 중 오류 발생: {e}")
        return
    
    # API 클라이언트 초기화
    api_client = NaverBlogSearchAPI(client_id, client_secret)
    
    # 결과를 저장할 리스트
    all_reviews = []
    
    # 진행 상황 추적
    total_libraries = len(library_df)
    
    # 이어서 작업할 수 있도록 기존 파일 확인
    processed_libraries = set()
    if os.path.exists(output_path):
        try:
            existing_df = pd.read_csv(output_path)
            processed_libraries = set(existing_df['도서관명'].unique())
            print(f"기존 파일에서 {len(processed_libraries)}개 도서관 처리 기록 발견")
        except Exception as e:
            print(f"기존 파일 읽기 실패, 처음부터 시작합니다: {e}")
    
    # CSV 파일 열기 (append 모드)
    with open(output_path, 'a', newline='', encoding='utf-8-sig') as f:
        # CSV 작성자 초기화
        fieldnames = ['도서관명', '블로그제목', '블로그내용', '블로그링크', '블로그이름', '포스팅날짜']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        # 파일이 비어있으면 헤더 작성
        if os.path.getsize(output_path) == 0:
            writer.writeheader()
        
        # 각 도서관에 대해 리뷰 수집
        for idx, row in library_df.iterrows():
            library_name = row['도서관명']
            
            # 이미 처리된 도서관 건너뛰기
            if library_name in processed_libraries:
                print(f"[{idx+1}/{total_libraries}] {library_name} - 이미 처리됨, 건너뜁니다.")
                continue
            
            print(f"[{idx+1}/{total_libraries}] {library_name} 처리 중...")
            
            # 블로그 리뷰 수집
            reviews = api_client.get_all_blog_reviews(
                library_name, 
                max_reviews=reviews_per_library
            )
            
            # 결과 저장
            for review in reviews:
                writer.writerow(review)
                f.flush()  # 즉시 디스크에 쓰기
            
            all_reviews.extend(reviews)
            
            # 진행 상황 출력
            if (idx + 1) % 10 == 0:
                print(f"진행 상황: {idx+1}/{total_libraries} 도서관 처리 완료")
            
            # API 호출 제한을 고려한 지연
            time.sleep(delay_between_libraries)
    
    print(f"총 {len(all_reviews)}개의 블로그 리뷰를 수집하여 {output_path}에 저장했습니다.")
    return all_reviews


if __name__ == "__main__":
    # 파일 경로 설정
    library_data_path = 'library_info_cleaned.csv'
    output_path = 'library_blog_reviews_api.csv'
    
    # 네이버 API 키 설정 
    client_id = "MhwFqrTamD_qfZ24piL6"
    client_secret = "lAQ4RNsifH"
    
    # 리뷰 수집 실행
    collect_library_reviews(
        library_data_path=library_data_path,
        output_path=output_path,
        client_id=client_id,
        client_secret=client_secret,
        reviews_per_library=5,  # 도서관당 수집할 리뷰 수
        delay_between_libraries=1  # 도서관 간 지연 시간(초)
    )
