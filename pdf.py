import pdfplumber
import re 
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("PDFParser").getOrCreate()

pdf_text = []
with pdfplumber.open(r"C:\Projects\HMC\IP\DeepakGuptaCV.pdf") as pdf:
    for page in pdf.pages:
        pdf_text.append(page.extract_text())

rows = []
for text in pdf_text:
    if text:
        for line in text.split('\n'):
            match = re.match(r"(\d+)\s+(\w+)\s+(\d+)", line)
            if match:
                rows.append(Row(id=int(match.group(1)), name=match.group(2), value=int(match.group(3))))

df = spark.createDataFrame(rows)
df.show()
