"""Tests de la vectorización (include/leyes/embed.py)."""
import importlib
import json
import math
import types as pytypes
import unittest.mock as mock

import pytest

import airflow.sdk

MOCK_API_KEY = "fake-key"
MOCK_VECTOR_DIM = 3


class FakeContentEmbedding:
    def __init__(self, values):
        self.values = values


class FakeEmbedContentResponse:
    def __init__(self, embeddings):
        self.embeddings = embeddings


class _ClientCalls:
    calls = []


class FakeModels:
    def embed_content(self, model=None, contents=None):
        _ClientCalls.calls.append((model, contents))
        return FakeEmbedContentResponse(
            [FakeContentEmbedding([0.1] * MOCK_VECTOR_DIM) for _ in contents]
        )


class FakeClient:
    def __init__(self, **kwargs):
        self.models = FakeModels()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _patch_genai():
    """Crea los módulos falsos de google.genai y retorna types_mod."""
    genai_mod = pytypes.ModuleType("google.genai")
    types_mod = pytypes.ModuleType("google.genai.types")
    types_mod.HttpOptions = lambda **kw: kw

    class FakePart:
        def __init__(self, text=None):
            self.text = text

    class FakeContent:
        def __init__(self, parts=None):
            self.parts = parts

    types_mod.Part = FakePart
    types_mod.Content = FakeContent
    genai_mod.types = types_mod
    genai_mod.Client = FakeClient

    google_pkg = pytypes.ModuleType("google")
    google_pkg.genai = genai_mod

    return {
        "google": google_pkg,
        "google.genai": genai_mod,
        "google.genai.types": types_mod,
    }, types_mod


MOCK_MD_TEXT = "# Titulo\n\n" + "\n\n".join(
    f"### Seccion {i}\n"
    + "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * 8
    for i in range(400)
)


@pytest.fixture()
def mocked_genai_env():
    _ClientCalls.calls = []
    modules, types_mod = _patch_genai()

    with mock.patch.object(
        airflow.sdk.Variable,
        "get",
        staticmethod(lambda k, default=None: MOCK_API_KEY),
    ), mock.patch.object(
        __import__("pymupdf4llm"), "to_markdown", return_value=MOCK_MD_TEXT
    ), mock.patch.dict(
        "sys.modules", modules,
    ):
        yield types_mod


@pytest.fixture()
def mocked_genai_text_env():
    """Fixture para embed_text: no mockea pymupdf4llm."""
    _ClientCalls.calls = []
    modules, types_mod = _patch_genai()

    with mock.patch.object(
        airflow.sdk.Variable,
        "get",
        staticmethod(lambda k, default=None: MOCK_API_KEY),
    ), mock.patch.dict(
        "sys.modules", modules,
    ):
        yield types_mod


# --- embed_pdf (antes embed_mocion) ---


def test_embed_pdf_passes_one_content_per_chunk(mocked_genai_env):
    """Cada chunk debe enviarse como su propio Content (gemini-embedding-2 no
    fusiona list[str]; un Content de varias partes produce UN embedding)."""
    import include.leyes.embed as embed
    importlib.reload(embed)

    result = embed.embed_pdf({"path": "fake.pdf", "id_proyecto": "12345", "titulo": "Ley Test"})

    chunks = json.loads(result["chunks"])
    assert chunks
    assert len(chunks) == sum(len(c[1]) for c in _ClientCalls.calls)

    for model, contents in _ClientCalls.calls:
        assert model == embed.EMBEDDING_MODEL
        assert len(contents) <= embed.EMBED_BATCH_SIZE
        for content in contents:
            assert isinstance(content, mocked_genai_env.Content)
            assert isinstance(content.parts, list)
            assert len(content.parts) == 1
            assert content.parts[0].text


def test_embed_pdf_batches_chunks(mocked_genai_env):
    """El lote de chunks se reparte en llamadas de a EMBED_BATCH_SIZE."""
    import include.leyes.embed as embed
    importlib.reload(embed)

    result = embed.embed_pdf({"path": "fake.pdf", "id_proyecto": "12345", "titulo": "Ley Test"})

    total = len(json.loads(result["chunks"]))
    expected_calls = math.ceil(total / embed.EMBED_BATCH_SIZE)
    assert len(_ClientCalls.calls) == expected_calls


# --- embed_text ---


def test_embed_text_passes_one_content_per_chunk(mocked_genai_text_env):
    """embed_text: cada chunk de texto plano se envía como su propio Content."""
    import include.leyes.embed as embed
    importlib.reload(embed)

    result = embed.embed_text({
        "text": MOCK_MD_TEXT,
        "id_proyecto": "67890",
        "titulo": "Ley Test",
    })

    chunks = json.loads(result["chunks"])
    assert chunks
    assert all(fila["id_proyecto"] == "67890" for fila in chunks)
    assert len(chunks) == sum(len(c[1]) for c in _ClientCalls.calls)

    for model, contents in _ClientCalls.calls:
        assert model == embed.EMBEDDING_MODEL
        for content in contents:
            assert isinstance(content, mocked_genai_text_env.Content)
            assert isinstance(content.parts, list)
            assert len(content.parts) == 1
            assert content.parts[0].text


def test_embed_text_batches_chunks(mocked_genai_text_env):
    """embed_text: el texto se reparte en llamadas de a EMBED_BATCH_SIZE."""
    import include.leyes.embed as embed
    importlib.reload(embed)

    result = embed.embed_text({
        "text": MOCK_MD_TEXT,
        "id_proyecto": "67890",
        "titulo": "Ley Test",
    })

    filas = json.loads(result["chunks"])
    total = len(filas)
    expected_calls = math.ceil(total / embed.EMBED_BATCH_SIZE)
    assert len(_ClientCalls.calls) == expected_calls
    assert all(fila["id_proyecto"] == "67890" for fila in filas)
