import io
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple, Literal

import pandas as pd
from haystack import Document, logging, component
from haystack.dataclasses import ByteStream
from haystack.components.converters.utils import get_bytestream_from_source, normalize_metadata

logger = logging.getLogger(__name__)


@component
class PandasExcelToDocument:
    def __init__(
        self,
        table_format: Literal["csv", "markdown"] = "csv",
        table_format_kwargs: Optional[Dict[str, Any]] = None
    ):
        """
        Create a PandasExcelToDocument component.

        :param table_format: The format to convert the Excel file to.
        :param table_format_kwargs: Additional keyword arguments to pass to the table format function.
        """
        self.table_format = table_format
        self.table_format_kwargs = table_format_kwargs or {}

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
                tables, tables_metadata = self._extract_tables(bytestream)
            except Exception as e:
                logger.warning(
                    "Could not read {source} and convert it to a pandas dataframe, skipping. Error: {error}",
                    source=source,
                    error=e,
                )
                continue

            # Loop over tables and create a Document for each table
            for table, excel_metadata in zip(tables, tables_metadata):
                merged_metadata = {
                    **bytestream.meta,
                    **metadata,
                    **excel_metadata
                }
                document = Document(content=table, meta=merged_metadata)
                documents.append(document)

        return {"documents": documents}

    def _extract_tables(self, bytestream: ByteStream) -> Tuple[List[str], List[Dict]]:
        """
        Extract tables from a Excel file.
        """
        df_dict = pd.read_excel(
            io=io.BytesIO(bytestream.data),
            header=None,  # Don't assign any pandas column labels
            sheet_name=None,  # Loads all sheets
            engine="openpyxl",  # Use openpyxl as the engine to read the Excel file
        )

        # Drop all columns and rows that are completely empty
        for key in df_dict:
            df = df_dict[key]
            df = df.dropna(axis=1, how="all", ignore_index=True)
            df = df.dropna(axis=0, how="all", ignore_index=True)
            df_dict[key] = df

        tables = []
        metadata = []
        for key in df_dict:
            if self.table_format == "csv":
                resolved_kwargs = {
                    "index": False,
                    "header": False,
                    **self.table_format_kwargs,
                }
                tables.append(
                    df_dict[key].to_csv(**resolved_kwargs)
                )
            elif self.table_format == "markdown":
                resolved_kwargs = {
                    "index": False,
                    "headers": (),
                    "tablefmt": "pipe",  # tablefmt 'plain', 'simple', 'grid', 'pipe', 'orgtbl', 'rst', 'mediawiki',
                                         # 'latex', 'latex_raw', 'latex_booktabs', 'latex_longtable' and tsv
                    **self.table_format_kwargs,
                }
                tables.append(
                    df_dict[key].to_markdown(**resolved_kwargs)
                )
            else:
                raise ValueError(f"Unsupported export format: {self.table_format}. Choose either 'csv' or 'markdown'.")
            metadata.append({"sheet_name": key})
        return tables, [{}]
