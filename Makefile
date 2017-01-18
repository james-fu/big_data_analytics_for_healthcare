.PHONY: new_project

new_project:
	mkdir -p ./projects/new_project
	cd ./projects/project1; find . -type d > ../new_project/dirs.txt
	cd ./projects/new_project; xargs mkdir -p < dirs.txt
	cd ./projects/new_project; xargs -I '{}' touch '{}'/.gitkeep < dirs.txt
	cd ./projects/new_project; rm dirs.txt
	touch ./projects/new_project/README.md
