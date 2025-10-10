import os, runpy
os.chdir(os.path.dirname(__file__))  # garante caminhos relativos
runpy.run_path("sistema.py", run_name="__main__")
