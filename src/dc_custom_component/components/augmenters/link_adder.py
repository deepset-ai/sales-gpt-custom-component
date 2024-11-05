from typing import List

import pandas as pd
import re

from haystack import Answer, Document, logging, component


logger = logging.getLogger(__name__)

@component
class ExternalLinkAdder:
    @component.output_types(documents=List[Answer])
    def run(self, answers: List[Answer]):
        for answer in answers:
            external_links = []
            if references:= answer.meta.get('_references', None):
                for reference in references:
                    document_id = reference['document_id']
                    for document in answer.documents:
                        if document.id == document_id:
                            src_url = document.meta.get('src_url', '')
                            link_name = document.meta.get('file_name', '')
                            if "spreadsheets" in src_url:
                                sheet_name = document.meta.get('sheet_name', '')
                                link_name += f"#{sheet_name}"
                                sheet_id = document.meta.get('sheet_name_id_map', {}).get(sheet_name, '')
                                src_url += f"?gid={sheet_id}#gid={sheet_id}"
                                match = re.search(r"{Row (\d+)}", answer.data[reference['answer_start_idx']:])
                                if match:
                                    row = match.group(1)
                                    link_name += f"!{row}:{row}"
                                    src_url += f"&range={row}:{row}"
                            external_links.append((link_name, src_url))
                            break
            if external_links:
                external_link_str = ""
                for i, (link_name, external_link) in enumerate(external_links):
                    external_link_str += f"\n\n[[Ext {i+1}]{link_name}]({external_link})"
                answer.data = answer.data + external_link_str

        return {"answers": answers}