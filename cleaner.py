import logging


logger = logging.getLogger(__name__)


def clean(data):
    data = data.replace('<br/>', ' ').strip()
    logger.debug(data)
    # data = ' '.join(data.split())
    return data


def clean_data(results, booklet_type):
    for result_dict in reversed(results['Results']):
        data = result_dict['Data']
        if len(data['Document']) > 1:
            logger.error(f'data has more than 1 document {data["Document"][0]["DisplayName"]}')
            exit()

        if 'תיקונים עקיפים:' in data['DocSummary']['DescriptionHtmlString']:
            summary, description = data['DocSummary']['DescriptionHtmlString'].split('תיקונים עקיפים:')
            display_name = data['Document'][0]['DisplayName'].replace('<br/>', '')
            description = description.strip('<br/>')
            description = f"{display_name}<br/><br/>{description}"
        else:
            description = data['Document'][0]['DisplayName']
            summary = data['DocSummary']['DescriptionHtmlString']

        print(f'summary: {summary}')
        print(f'description: {description}')
        datum = {
            'creation_date': data['CreationDate'],
            'modify_date': data['ModifyDate'],
            'number_of_pages': data['Pages'],
            'published_date': data['PublishDate'],
            'file_name': data['Document'][0]['FileName'],
            'display_name': data['Document'][0]['DisplayName'],
            'extension': data['Document'][0]['Extension'],
            'description': data['DocSummary']['DescriptionHtmlString'],
            'booklet_number': data['BookletNum'],
            'foreign_year': data['ForeignYear'],
            'booklet_type': booklet_type
        }
        yield datum
