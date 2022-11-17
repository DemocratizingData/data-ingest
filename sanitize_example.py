from pathlib import Path
from gzip import GzipFile
import json

pub_years = {}
for infile in Path('test/example_data/agency/version/json/publications/').glob('*.json.gz'):
    with GzipFile(infile, 'r') as f:
        json_lines = [json.loads(i) for i in f.readlines()]
    for pub in json_lines:
        for dataset in pub['identified_datasets']:
            for i in range(len(dataset['snippets'])):
                dataset['snippets'][i] = dataset['snippets'][i][:10]
        pyear = pub['publication_year']
        pub_years[pyear] = pub_years.get(pyear, 0) + 1
    with GzipFile(infile, 'w') as f:
        f.write('\n'.join(json.dumps(i) for i in json_lines).encode('utf-8'))

with open('test/example_data/agency/version/stat/export_metadata.json', 'r') as f:
    stats = json.load(f)
    stats['stats']['overall']['unique_documents_per_year'] = [{'publication_year': y, 'documents': c} for y,c in pub_years.items()]

with open('test/example_data/agency/version/stat/export_metadata.json', 'w') as f:
    json.dump(stats, f)
