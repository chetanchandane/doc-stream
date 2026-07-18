"""CQRS read-side projection.

The projector consumes ``documents.enriched`` and maintains the
``document_view`` read model that the query service serves from.
"""
