import logging

logger = logging.getLogger(__name__)


def clean(data):
    data = data.replace('<br/>', ' ').strip()
    logger.debug(data)
    return data


def clean_data(results, booklet_type):
    split_texts = ["תיקונים עקיפים:", "תיקון עקיף:"]  # Define the texts to split the description

    for result_dict in reversed(results['Results']):
        data = result_dict['Data']
        if len(data['Document']) > 1:
            logger.error(f'data has more than 1 document {data["Document"][0]["DisplayName"]}')
            exit()

        description_html = data['DocSummary']['DescriptionHtmlString']
        split_text = next((text for text in split_texts if text in description_html), None)

        if split_text:
            summary, description = description_html.split(split_text)
            doc_summary = [summary.replace('<br/>', '')]
            display_name = data['Document'][0]['DisplayName'].replace('<br/>', '\n')
            description = description.strip('<br/>').replace('<br/>', '\n')
            description = f"{display_name}\n\n{description}"
        else:
            description = data['Document'][0]['DisplayName'].replace('<br/>', '\n')
            doc_summary = data['DocSummary']['DescriptionHtmlString'].split('<br/>')
            doc_summary = [summary.strip() for summary in doc_summary if summary.strip()]

        for summary in doc_summary:
            datum = {
                'creation_date': data['CreationDate'],
                'modify_date': data['ModifyDate'],
                'number_of_pages': data['Pages'],
                'published_date': data['PublishDate'],
                'file_name': data['Document'][0]['FileName'],
                'display_name': summary,
                'extension': data['Document'][0]['Extension'],
                'description': description,
                'booklet_number': data['BookletNum'],
                'foreign_year': data['ForeignYear'],
                'booklet_type': booklet_type
            }
            yield datum
