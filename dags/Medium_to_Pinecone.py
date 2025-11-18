from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd
import os, time, requests, ast, json
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec

# ---- Config from Airflow Variables ----
API_KEY = Variable.get("PINECONE_API_KEY")
CLOUD = Variable.get("PINECONE_CLOUD", default_var="aws")
REGION = Variable.get("PINECONE_REGION", default_var="us-east-1")
INDEX_NAME = Variable.get("PINECONE_INDEX_NAME", default_var="semantic-search-fast")
DIM = int(Variable.get("PINECONE_DIM", default_var="384"))
METRIC = Variable.get("PINECONE_METRIC", default_var="dotproduct")  # slides use dotproduct
DATA_URL = Variable.get(
    "DATA_URL",
    default_var="https://s3-geospatial.s3.us-west-2.amazonaws.com/medium_data.csv",
)

DATA_DIR = "/opt/airflow/data/medium"
RAW_CSV = f"{DATA_DIR}/medium_data.csv"
PREP_CSV = f"{DATA_DIR}/medium_preprocessed.csv"
RESULTS_JSON = f"{DATA_DIR}/search_results.json"

default_args = dict(
    owner="airflow",
    depends_on_past=False,
    retries=1,
    retry_delay=timedelta(minutes=5),
)

with DAG(
    dag_id="Medium_to_Pinecone",
    description="Medium article recommendation: embeddings + Pinecone",
    default_args=default_args,
    schedule_interval=timedelta(days=7),
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=["medium", "pinecone", "search-engine"],
) as dag:

    @task
    def download_data() -> str:
        """Download the CSV to the mounted data folder."""
        os.makedirs(DATA_DIR, exist_ok=True)
        r = requests.get(DATA_URL, stream=True, timeout=120)
        r.raise_for_status()
        with open(RAW_CSV, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                if chunk:
                    f.write(chunk)
        with open(RAW_CSV, "r", encoding="utf-8", errors="ignore") as f:
            print("Lines:", sum(1 for _ in f))
        return RAW_CSV

    @task
    def preprocess_data(path: str) -> str:
        """Create text + metadata and write a slim CSV for embedding."""
        df = pd.read_csv(path)
        df["title"] = df["title"].astype(str).fillna("")
        df["subtitle"] = df["subtitle"].astype(str).fillna("")
        df["text"] = (df["title"] + " " + df["subtitle"]).str.strip()
        df["id"] = df.reset_index(drop=True).index.astype(str)
        df["metadata"] = df["text"].apply(lambda t: {"title": t})
        df[["id", "text", "metadata"]].to_csv(PREP_CSV, index=False)
        print(f"Saved {PREP_CSV} rows={len(df)}")
        return PREP_CSV

    @task
    def create_pinecone_index() -> str:
        """Create the serverless index if missing; wait until ready."""
        pc = Pinecone(api_key=API_KEY)
        if INDEX_NAME not in [ix["name"] for ix in pc.list_indexes()]:
            pc.create_index(
                name=INDEX_NAME,
                dimension=DIM,
                metric=METRIC,  # 'dotproduct' per slides
                spec=ServerlessSpec(cloud=CLOUD, region=REGION),
            )
            print(f"Requested creation of index '{INDEX_NAME}'")
        # wait until ready
        while True:
            desc = pc.describe_index(name=INDEX_NAME)
            if desc.get("status", {}).get("ready", False):
                break
            time.sleep(1)
        print(f"Index '{INDEX_NAME}' ready.")
        return INDEX_NAME

    @task
    def generate_embeddings_and_upsert(prep_path: str, index_name: str) -> str:
        """Embed text with MiniLM-L6-v2 and upsert to Pinecone."""
        df = pd.read_csv(prep_path)
        if df["metadata"].dtype == object:
            df["metadata"] = df["metadata"].apply(
                lambda x: x if isinstance(x, dict) else ast.literal_eval(str(x))
            )

        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device="cpu")

        pc = Pinecone(api_key=API_KEY)
        index = pc.Index(index_name)

        batch = 200
        for s in range(0, len(df), batch):
            chunk = df.iloc[s : s + batch].copy()
            texts = chunk["text"].tolist()
            embs = model.encode(texts, batch_size=64, convert_to_numpy=True)
            vectors = [
                {"id": str(row.id), "values": embs[i].tolist(), "metadata": row.metadata}
                for i, row in enumerate(chunk.itertuples(index=False))
            ]
            index.upsert(vectors=vectors)
            print(f"Upserted {len(vectors)} vectors ({s}–{s+len(vectors)})")
        return index_name

    @task
    def test_search_query(index_name: str) -> None:
        """Encode a query, hit Pinecone, and save JSON results."""
        pc = Pinecone(api_key=API_KEY)
        index = pc.Index(index_name)

        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device="cpu")
        query = "what is ethics in AI"
        qvec = model.encode(query).tolist()

        res = index.query(vector=qvec, top_k=5, include_metadata=True, include_values=False)

        payload = res.to_dict()  # make it JSON-serializable
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(RESULTS_JSON, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        print(f"Search results for query: '{query}'")
        for m in payload.get("matches", []):
            title = (m.get("metadata", {}) or {}).get("title", "")[:80]
            print(f"ID={m.get('id')}  score={m.get('score'):.4f}  |  {title}")

    # ---- Task flow ----
    raw = download_data()
    prep = preprocess_data(raw)
    idx = create_pinecone_index()
    final_idx = generate_embeddings_and_upsert(prep, idx)
    test_search_query(final_idx)
