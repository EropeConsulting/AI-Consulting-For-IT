import pdfplumber
from neo4j import GraphDatabase
import re
from typing import List, Dict, Tuple

# --- 1. 환경 설정 ---

# Neo4j 데이터베이스 연결 정보 설정
# 실제 운영 환경에 맞게 URI, USER, PASSWORD를 변경해야 합니다.
URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "your_neo4j_password" 

# --- 2. Neo4j 드라이버 클래스 ---

class Neo4jConnector:
    """Neo4j 데이터베이스 연결 및 쿼리 실행을 위한 클래스"""
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        print("Neo4j 드라이버 연결 성공.")

    def close(self):
        """드라이버 연결 종료"""
        self.driver.close()
        print("Neo4j 드라이버 연결 종료.")

    def execute_cypher(self, cypher_query, parameters=None):
        """
        Cypher 쿼리를 실행하고 결과를 반환합니다.
        (주로 MERGE/CREATE 작업에 사용)
        """
        with self.driver.session() as session:
            try:
                # 쿼리를 실행하고 결과 레코드의 수를 반환
                result = session.run(cypher_query, parameters if parameters else {})
                summary = result.consume()
                return summary.counters
            except Exception as e:
                print(f"ERROR: 쿼리 실행 중 오류 발생: {e}")
                print(f"쿼리: {cypher_query}")
                return None

# --- 3. 지식 추출 및 변환 (Transform) 로직 ---

def extract_text_from_pdf(pdf_path: str) -> str:
    """PDF 파일 경로에서 텍스트를 추출합니다 (1단계: 추출)"""
    full_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                full_text += page.extract_text() + "\n\n"
        print(f"PDF 파일 '{pdf_path}'에서 텍스트 추출 완료.")
        return full_text
    except Exception as e:
        print(f"ERROR: PDF 파일 처리 중 오류 발생: {e}")
        return ""

def mock_nlp_to_triples(text: str) -> List[Tuple[str, str, str, str, str]]:
    """
    NLP 기능을 모의 구현하여 텍스트에서 개체와 관계를 추출합니다.
    (2단계: 변환)
    
    실제로는 Konlpy, spaCy, 또는 LLM API를 통해 NER/Relation Extraction이 수행됩니다.
    여기서는 간단한 정규 표현식 규칙 기반으로 핵심 키워드를 추출합니다.
    """
    print("\n[NLP 모의 분석] 텍스트에서 핵심 삼중항 추출 시작...")
    
    # IT 컨설팅 보고서에서 자주 나오는 키워드와 관계를 가정합니다.
    # 튜플 형식: (노드1 타입, 노드1 이름, 관계, 노드2 타입, 노드2 이름)
    triples = []
    
    # 1. '프로젝트'와 '기술' 관계 추출
    project_keywords = re.findall(r"(프로젝트 [A-Z]{1,3})은 (.+? 기술)을", text)
    for proj, tech in project_keywords:
        triples.append(("Project", proj.strip(), "USES_TECH", "Technology", tech.strip()))

    # 2. '회사'와 '프로젝트' 관계 추출
    company_proj_keywords = re.findall(r"(\w+? 컴퍼니)에서 (프로젝트 [A-Z]{1,3})를 수행", text)
    for comp, proj in company_proj_keywords:
        triples.append(("Company", comp.strip(), "CONDUCTS", "Project", proj.strip()))

    # 3. '회사'와 '산업군' 관계 추출
    industry_keywords = re.findall(r"(\w+? 컴퍼니)는 (.+? 산업) 분야의", text)
    for comp, ind in industry_keywords:
        triples.append(("Company", comp.strip(), "BELONGS_TO", "Industry", ind.strip()))
        
    print(f"총 {len(triples)}개의 잠재적 삼중항 추출 완료.")
    return triples

def generate_cypher_query(triples: List[Tuple[str, str, str, str, str]]) -> str:
    """
    추출된 삼중항을 Neo4j MERGE 쿼리 문자열로 변환합니다.
    (2단계: 변환)
    """
    cypher_parts = []
    for n1_type, n1_name, relation, n2_type, n2_name in triples:
        # 노드 이름을 쿼리에 직접 삽입하는 대신 파라미터로 처리하는 것이 더 안전하지만,
        # 여기서는 설명을 위해 단순 문자열 포맷팅을 사용합니다.
        
        # 노드 이름의 특수 문자 문제 방지를 위해 replace 처리 (간단화)
        n1_name_safe = n1_name.replace("'", "\\'")
        n2_name_safe = n2_name.replace("'", "\\'")

        cypher = f"""
        MERGE (n1:{n1_type} {{name: '{n1_name_safe}'}})
        MERGE (n2:{n2_type} {{name: '{n2_name_safe}'}})
        MERGE (n1)-[:{relation}]->(n2);
        """
        cypher_parts.append(cypher)
        
    return "\n".join(cypher_parts)

# --- 4. 메인 실행 함수 ---

def automated_kg_pipeline(pdf_path: str, neo4j_conn: Neo4jConnector):
    """
    PDF 파일에서 지식 그래프로 변환하는 전체 파이프라인 실행
    """
    print("=" * 50)
    print(f"지식 그래프 자동화 파이프라인 시작: {pdf_path}")
    print("=" * 50)

    # 1. 데이터 추출 (PDF -> 텍스트)
    document_text = extract_text_from_pdf(pdf_path)
    if not document_text:
        return

    # 2. 의미 분석 및 삼중항 추출 (텍스트 -> 삼중항 리스트)
    triples_list = mock_nlp_to_triples(document_text)
    if not triples_list:
        print("추출된 유효한 관계가 없습니다. 종료합니다.")
        return

    # 3. Cypher 쿼리 생성
    full_cypher_query = generate_cypher_query(triples_list)
    
    # (선택적) 생성된 쿼리 파일로 저장 (디버깅용)
    with open("generated_cypher.cypher", "w", encoding="utf-8") as f:
        f.write(full_cypher_query)
    print("\n[디버깅] 생성된 Cypher 쿼리 'generated_cypher.cypher'에 저장 완료.")

    # 4. Neo4j 로딩 (쿼리 실행)
    print("\n[Neo4j 로딩] 지식 그래프에 데이터 삽입 시작...")
    
    # 생성된 쿼리를 한 번에 실행 (효율성을 위해)
    # 실제로는 트랜잭션으로 묶어 처리하는 것이 안전합니다.
    counters = neo4j_conn.execute_cypher(full_cypher_query)

    if counters:
        print("\n✅ 데이터 삽입 성공!")
        print(f"  > 생성된 노드: {counters.nodes}")
        print(f"  > 생성된 관계: {counters.relationships}")
        print("=" * 50)
    else:
        print("\n❌ 데이터 삽입 실패. 로그를 확인하세요.")
        print("=" * 50)

# --- 5. 실행 예시 ---

if __name__ == "__main__":
    # 📌 실제 사용할 PDF 파일 경로로 변경하세요.
    PDF_FILE_PATH = "IT_Consulting_Report_Sample.pdf" 
    
    # 📌 Neo4j 연결 정보의 비밀번호를 설정하세요.
    # PASSWORD = "your_neo4j_password"

    # 테스트를 위한 가상 PDF 파일 생성 (실제 파일이 없어도 테스트 가능)
    # 실제 PDF 보고서의 내용과 유사한 구조의 텍스트를 사용합니다.
    sample_content = f"""
    ### 2025년 Q3 IT 컨설팅 보고서: A 컴퍼니 전략 ###
    
    서론: A 컴퍼니는 금융 산업 분야의 선두 주자입니다.
    
    1. 프로젝트 AAA 분석: 
    프로젝트 AAA은 Python 기술을 사용하며, 주니어 컨설턴트 3명이 수행했습니다.
    
    2. 프로젝트 BBB 분석:
    프로젝트 BBB은 AWS 기술을 사용하며, B 컴퍼니에서 프로젝트 BBB를 수행했습니다. 이 프로젝트는 인공지능 산업 분야의 중요한 레퍼런스입니다.
    
    3. 프로젝트 CCC 분석:
    C 컴퍼니에서 프로젝트 CCC를 수행했으며, 프로젝트 CCC은 Java 기술을 사용했습니다.
    """

    try:
        with open(PDF_FILE_PATH, "w", encoding="utf-8") as f:
            f.write(sample_content)
    except Exception as e:
        # pdfplumber는 실제 .pdf 파일을 기대하므로, 가짜 파일로 대체합니다.
        # 실제 환경에서는 PDF_FILE_PATH에 실제 PDF 경로를 넣고 이 부분을 제거하세요.
        print(f"주의: 실제 PDF 파일이 필요합니다. 테스트를 위해 임시 텍스트 사용.")


    # 1. Neo4j 연결 설정
    connector = Neo4jConnector(URI, USER, PASSWORD)
    
    # 2. 파이프라인 실행
    # 실제로는 PDF_FILE_PATH의 PDF 내용을 파싱하게 됩니다.
    automated_kg_pipeline(PDF_FILE_PATH, connector)

    # 3. 연결 종료
    connector.close()