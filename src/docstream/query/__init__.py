"""CQRS read side: the query service.

Serves semantic search and grounded question answering from the ``document_view``
read model plus Qdrant. Never writes, and never reads the ``jobs`` write model.
"""
