.PHONY: run clean 

hello:
	echo "Hello, World"
	echo "This line will print if the file hello does not exist"

# run:
# 	python app.py

# setup: requirements.txt
# 	pip install -r requirements.txt

# clean:
# 	rm -rf __pycache

.\venv\Scripts\activate: requirements.txt
	python -m venv venv
	.\venv\Scripts\pip install -r requirements.txt

run: .\venv\Scripts\activate
	.\venv\Scripts\python app.py

clean: 
	rm -rf __pycache__
	rm -rf venv 

VENV = venv 
PYTHON = $(VENV)\Scripts\python
PIP = $(VENV)\Scripts\pip 

run: $(VENV)\Scripts\activate
	$(PYTHON) app.py

$(VENV)\Scripts\activate: requirements.txt
	python -m venv $(VENV)
	$(PIP) install -r requirements.txt 

clean:
	rm -rf __pycache__
	rm -rf $(VENV)




# https://earthly.dev/blog/python-makefile/
# make consists of a set of rules 
# each rule has 3 parts - a target, a list of prerequisites, and a recipe
# target represents a goal to be achieved - usually a file that needs to be created in the build
# the prerequisite list tells make which files are this target dependent on - prerequisite can be a file or another target
#  recipes are a list of shell commands that will be executed by make as part of building the target
#  	When make executes a target, it looks at its prerequisites. 
#  	If those prerequisites have their own recipes, make executes them and when all the prerequisites are ready for a target, 
# it executes the corresponding recipe for the current target.