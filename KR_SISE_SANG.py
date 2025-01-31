import requests
import pandas as pd
from datetime import datetime
import line_alert
import os

def fetch_upper_limit_stocks():
    """네이버 금융에서 상한가 종목 데이터를 가져오는 함수"""
    
    def get_stock_codes():
        """종목코드 딕셔너리를 생성하는 함수"""
        # 종목코드 URL
        urls = {
            'KOSPI': "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt",
            'KOSDAQ': "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt"
        }
        
        # 종목코드 데이터프레임 생성 및 합치기
        code_df = pd.concat([
            pd.read_html(url, encoding='cp949')[0] 
            for url in urls.values()
        ])
        
        # 종목코드 6자리로 맞추기
        code_df['종목코드'] = code_df['종목코드'].astype(str).str.zfill(6)
        
        return dict(zip(code_df['회사명'], code_df['종목코드']))
    
    # 기본 설정
    urls = {
        'KOSPI': 'https://finance.naver.com/sise/sise_upper.naver',
        'KOSDAQ': 'https://finance.naver.com/sise/sise_upper.naver?sosok=1'
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    # 데이터 수집
    all_stocks = []
    processed_stocks = set()
    
    for market, url in urls.items():
        try:
            response = requests.get(url, headers=headers)
            response.encoding = 'euc-kr'
            dfs = pd.read_html(response.text)
            
            for df in dfs:
                if len(df.columns) > 4 and '종목명' in df.columns:
                    # 데이터 전처리
                    df = df.dropna(subset=['종목명']).astype(str)
                    df = df[df['종목명'].str.contains('^[가-힣]+', na=False)]
                    df = df[~df['종목명'].isin(processed_stocks)]
                    
                    if len(df) > 0:
                        processed_stocks.update(df['종목명'].tolist())
                        
                        # 필요한 컬럼만 선택
                        columns_to_keep = [
                            '종목명', '현재가', '등락률', '거래량', 
                            '시가', '고가', '저가', 'PER',
                            'N', '연속', '누적'
                        ]
                        df = df[columns_to_keep]
                        all_stocks.append(df)
        
        except Exception as e:
            print(f"{market} 데이터 수집 중 오류 발생: {e}")
            continue
    
    if not all_stocks:
        print("수집된 데이터가 없습니다.")
        return None
    
    # 모이터 병합 및 전처리
    result_df = pd.concat(all_stocks, ignore_index=True)
    
    # 거래일자 컬럼 추가
    current_date = datetime.now().strftime("%Y%m%d")
    result_df.insert(0, '거래일자', current_date)
    
    # 종목코드 매핑
    code_dict = get_stock_codes()
    result_df['종목코드'] = result_df['종목명'].map(code_dict)
    
    # 숫자 데이터 정리
    numeric_columns = ['현재가', '등락률', '거래량', '시가', '고가', '저가', 'PER', 'N', '연속', '누적']
    for col in numeric_columns:
        if col == '등락률':
            result_df[col] = result_df[col].astype(str).str.replace(',', '')
            result_df[col] = result_df[col].str.replace('+', '').str.replace('-', '-').str.replace('%', '')
        else:
            result_df[col] = result_df[col].astype(str).str.replace(',', '')
        result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
    
    # 컬럼 순서 재정렬
    column_order = [
        '거래일자', 'N', '연속', '누적', '종목코드', '종목명', 
        '현재가', '등락률', '거래량', '시가', '고가', '저가', 'PER'
    ]
    result_df = result_df[column_order]
    
    # 결과 저장
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'./sise_csv/upper_limit_stocks_{current_time}.csv'
    
    # sise_csv 폴더가 없으면 생성
    os.makedirs('sise_csv', exist_ok=True)
    
    result_df.to_csv(filename, index=False, encoding='utf-8-sig')
    
    print(f"\n데이터가 {filename}에 저장되었습니다.")
    print(f"총 {len(result_df)}개 상한가 종목이 저장되었습니다.")
    message = f"총 {len(result_df)}개 상한가 종목이 저장되었습니다.\n\n"
    message += "상한가 종목:\n"
    for idx, row in result_df.iterrows():
        message += f"{row['종목명']}\n"

    line_alert.SendMessage(message)
    
    return result_df


if __name__ == "__main__":
    print("상한가 종목 데이터 수집을 시작합니다...")
    df = fetch_upper_limit_stocks()
    
    if df is not None:
        print("\n상한가 종목 목록:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.float_format', lambda x: '{:,.2f}'.format(x) if pd.notnull(x) else '')
        print(df.to_string(index=False))
