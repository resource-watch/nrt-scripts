# Python scripts in docker environment
To create your own script, you do a fork of this repository and change the next variable:

## Init
In start.sh change the NAME environment variable with the name of your application.

## Develop

You can develop all your code inside the src folder. Always the script execute the __init__.py file.

If you need share files with your scripts files, you can put these in data folder. This folder is shared betwen docker and the host machine.
And if you need download the results of execution, you save these files in data folder too.

## Execution

Execute the next command in your console inside of your root folder (node or python)
```
./start.sh
```
