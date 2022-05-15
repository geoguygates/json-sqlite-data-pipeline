import os

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))

extract_bucket_pathname = os.path.join(ROOT_DIR, '2_project_code_output', 'extract_bucket')

transform_load_bucket_pathname = os.path.join(ROOT_DIR, '2_project_code_output', 'transform_load_bucket')