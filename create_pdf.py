from reportlab.pdfgen import canvas
from PyPDF2 import PdfReader, PdfWriter
import os

# PDF 파일 생성
pdf_path = "./test.pdf"
# pdf = canvas.Canvas(pdf_path)

# # 메타데이터 설정
# pdf.setTitle("테스트 PDF")
# pdf.setAuthor("작성자 이름")
# pdf.setSubject("PDF 생성 예제")
# pdf.setKeywords("파이썬, PDF, 예제")
# pdf.setCreator("PDF 생성자 이름")

# # PDF 내용 작성
# pdf.drawString(50, 800, "파이썬 PDF 파일 생성")

# # PDF 저장
# pdf.save()

# PyPDF2를 사용하여 PDF의 Producer 메타데이터 수정
with open(pdf_path, "rb") as file:
    reader = PdfReader(file)
    writer = PdfWriter()
    writer.append_pages_from_reader(reader)
    
    # 기존 메타데이터 가져오기
    metadata = reader.metadata
    new_metadata = {key: metadata[key] for key in metadata}
    
    # Producer 메타데이터 수정
    new_metadata["/Producer"] = "내가 설정한 Producer 이름"
    
    # 새로운 메타데이터 설정
    writer.add_metadata(new_metadata)
    
    # 수정된 PDF 저장
    modified_pdf_path = os.path.abspath("modified_test.pdf")
    with open(modified_pdf_path, "wb") as new_file:
        writer.write(new_file)

print(f"PDF 파일이 성공적으로 저장되었습니다: {modified_pdf_path}")