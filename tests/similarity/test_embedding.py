import pytest
import spark_fuse.similarity.embedding as embedding_mod

pytest.importorskip("pandas")


def test_sentence_embedding_generator_uses_stubbed_model(monkeypatch, spark):
    def _stub_loader(model_name, device):
        class _StubModel:
            def encode(self, values, batch_size, normalize_embeddings):
                return [[float(len(value))] for value in values]

        return _StubModel()

    monkeypatch.setattr(embedding_mod, "_load_sentence_model", _stub_loader)

    generator = embedding_mod.SentenceEmbeddingGenerator(
        input_col="text",
        output_col="vector",
        batch_size=4,
        normalize=False,
        drop_input=False,
    )

    df = spark.createDataFrame([(1, "hi"), (2, None)], ["id", "text"])

    result = generator.transform(df).orderBy("id").collect()

    assert result[0].vector == [2.0]
    assert result[1].vector == [0.0]
