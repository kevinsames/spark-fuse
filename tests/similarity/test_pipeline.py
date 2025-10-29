from pyspark.sql import functions as F

from spark_fuse.similarity import (
    CosineSimilarity,
    FirstItemChoice,
    IdentityEmbeddingGenerator,
    KMeansPartitioner,
    SimilarityPipeline,
)


def test_pipeline_clusters_points(spark):
    data = [
        (0, [0.0, 0.0]),
        (1, [0.1, -0.1]),
        (2, [9.9, 10.2]),
        (3, [10.4, 9.8]),
    ]
    df = spark.createDataFrame(data, ["id", "features"])

    pipeline = SimilarityPipeline(
        embedding_generator=IdentityEmbeddingGenerator(input_col="features"),
        partitioner=KMeansPartitioner(k=2, seed=42),
        similarity_metric=CosineSimilarity(embedding_col="embedding"),
    )

    clustered = pipeline.run(df)
    assignments = {row.id: row.cluster_id for row in clustered.select("id", "cluster_id").collect()}

    assert assignments[0] == assignments[1]
    assert assignments[2] == assignments[3]
    assert assignments[0] != assignments[2]


def test_choice_function_returns_one_rep_per_cluster(spark):
    data = [
        (0, 0, "a"),
        (1, 0, "b"),
        (2, 1, "c"),
        (3, 1, "d"),
    ]
    df = spark.createDataFrame(data, ["id", "cluster_id", "payload"])

    choice = FirstItemChoice(order_by=[F.col("payload").desc()])
    reps = choice.select(df)

    rows = reps.select("cluster_id", "payload").collect()
    as_dict = {row.cluster_id: row.payload for row in rows}

    assert as_dict[0] == "b"
    assert as_dict[1] == "d"
    assert len(as_dict) == 2
