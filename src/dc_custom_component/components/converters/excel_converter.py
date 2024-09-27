import io
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from haystack import Document, logging, component
from haystack.dataclasses import ByteStream
from haystack.components.converters.utils import get_bytestream_from_source, normalize_metadata

logger = logging.getLogger(__name__)


@component
class PandasExcelToDocument:
    def __init__(self):
        """
        Create a PandasExcelToDocument component.
        """
        pass

    @component.output_types(documents=List[Document])
    def run(
        self,
        sources: List[Union[str, Path, ByteStream]],
        meta: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
    ) -> Dict[str, List[Document]]:
        """
        Converts Excel files to Documents.

        :param sources:
            List of file paths or ByteStream objects.
        :param meta:
            Optional metadata to attach to the Documents.
            This value can be either a list of dictionaries or a single dictionary.
            If it's a single dictionary, its content is added to the metadata of all produced Documents.
            If it's a list, the length of the list must match the number of sources, because the two lists will
            be zipped.
            If `sources` contains ByteStream objects, their `meta` will be added to the output Documents.

        :returns:
            A dictionary with the following keys:
            - `documents`: Created Documents
        """
        documents = []
        meta_list = normalize_metadata(meta=meta, sources_count=len(sources))

        for source, metadata in zip(sources, meta_list):
            try:
                bytestream = get_bytestream_from_source(source)
            except Exception as e:
                logger.warning("Could not read {source}. Skipping it. Error: {error}", source=source, error=e)
                continue
            try:
                tables = self._extract_tables(bytestream)
            except Exception as e:
                logger.warning(
                    "Could not read {source} and convert it to a pandas dataframe, skipping. Error: {error}",
                    source=source,
                    error=e,
                )
                continue

            # TODO Loop over tables and create a Document for each table
            # TODO Is there an equivalent for excel?
            # excel_metadata = self._get_excel_metadata(document=file)
            merged_metadata = {
                **bytestream.meta,
                **metadata,
                # "excel": excel_metadata
            }
            document = Document(content=tables[0], meta=merged_metadata)
            documents.append(document)

        return {"documents": documents}

    def _extract_tables(self, bytestream: ByteStream) -> List[str]:
        """
        Extract tables from a Excel file.
        """
        df = pd.read_excel(
            io=io.BytesIO(bytestream.data),
            header=None,  # Don't assign any pandas column labels
            sheet_name=None,  # Loads all sheets
        )

        # Drop all columns and rows that are completely empty
        df = df.dropna(axis=1, how="all")
        df = df.dropna(axis=0, how="all")

        # TODO Add option to choose different formats eg. markdown
        text = df.to_csv(header=False, index=False)
        return [text]
