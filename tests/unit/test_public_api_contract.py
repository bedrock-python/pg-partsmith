from pg_partsmith.aio import maintain_partitions


def test__public_api__maintain_partitions__is_callable() -> None:
    # Arrange / Act / Assert
    assert callable(maintain_partitions)
