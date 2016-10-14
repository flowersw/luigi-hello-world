# Luigi Example

This is just a simple 'Hello World' example to demonstrate the use of Luigi.

It will create a file that says "Hello", another that says "World", and a third file that concatenates the contents of both. 

Usage:

```
pip install -r requirements.txt
luigid --background --port=8082 --log_path='./logs/'
python hello_world HelloWorldTask
python hello_world_with_parameters HelloWorldTask --execution_id='1234'
``