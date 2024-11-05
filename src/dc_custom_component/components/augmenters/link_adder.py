from typing import List

import pandas as pd
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
                            external_links.append(document.meta['src_url'])
                            break
            if external_links:
                external_link_str = ""
                for i, external_link in enumerate(external_links):
                    external_link_str += f"\n\n[Ext {i+1}]({external_link})"
                answer.data = answer.data + external_link_str

        return {"answers": answers}