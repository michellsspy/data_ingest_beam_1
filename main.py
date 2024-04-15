from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import os
import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s - %(levelname)s - %(message)s")

# Criando o pipeline options
pipeline_options = {
    'project': 'teste-templates-420021',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://teste-hello-word/staging',
    'temp_location': 'gs://teste-hello-word/temp',
    'template_location': 'gs://teste-hello-word/templates/template-hello',
    'save_main_session': True,
    'requirements_file': '/home/michel/Vídeos/Criando um template dataflow-beam/data_ingest_beam/requirements.txt'
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
pipeline = beam.Pipeline(options=pipeline_options)

class PrintHelloWord(beam.DoFn):
    def process(self, element):
        element = 'Hello Word!'
        #print(element)
        yield element


hello = (
    pipeline
    | beam.Create([None])
    | beam.ParDo(PrintHelloWord())
    | beam.Map(print)
)

pipeline.run()

