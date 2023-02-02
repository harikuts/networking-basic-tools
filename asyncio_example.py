"""
asyncio_example.py

Template provided by: Harikrishna Kuttivelil (UC Santa Cruz, Internetworking Research Group)

A simple example of using concurrent processing in Python, using basic features from asyncio.
This program is meant to serve as both an example and template for future projects.
You can find a good resource on asyncio here: https://realpython.com/async-io-python/.

Also in this program is the use of logging, which is a very useful tool for debugging and
tracking the progress of your program. You can find a good resource on logging here:
https://realpython.com/python-logging/ and here: https://docs.python.org/3/howto/logging.html.
"""

import asyncio
import datetime
import logging

"""
Setting up our logging.

Logging is essential for good programming and can be useful especially in a distributed setting.
They can also be used to track the progress of multiple programs and concurrent processes.
In essence, they work like fancy little print statements, but with more features and levels
of importance. Check the resources provided above for more information.
"""

# Setting up our default logging format.
logging.basicConfig(format='[%(asctime)s] (%(name)s) %(levelname)s: %(message)s',)
# Set up loggers for each of our concurrent functions.
logger_1 = logging.getLogger('func1')
logger_2 = logging.getLogger('func2')
logger_3 = logging.getLogger('func3')
# Set the logging level for each of our concurrent functions to INFO.
logger_1.setLevel(logging.INFO)
logger_2.setLevel(logging.INFO)
logger_3.setLevel(logging.INFO)
"""
We can also have different loggers, even across programs, log to the same file.
"""
# We will set up a common file handler for all of our loggers, and set it to INFO.
file_handler = logging.FileHandler('example.log')
file_handler.setLevel(logging.INFO)
# Add the file handler to each of our loggers.
logger_1.addHandler(file_handler)
logger_2.addHandler(file_handler)
logger_3.addHandler(file_handler)
"""
You can add many more handlers to your loggers, such as a stream handler, which will
print the log messages to the console.

Now with this set up, you can use 'tail -f example.log' within your terminal to see 
the log messages in real time, which is useful when using multiple concurrent programs.
"""

"""
Defining our concurrent functions.

Here, we will define several concurrent functions that will be used in our main function.
Because we want these functions to be concurrent, we will use the asyncio function 
declaration (async def), as opposed to the standard function declaration.
"""

# Function 1.
async def concurrent_function_1(interval=0.5):
    """
    A basic function that will print the time every interval seconds.
    """
    # We will use a while loop to keep the function running.
    while True:
        """
        Carry out your function here. For now, we will just log the time.
        """
        # Print the time.
        logger_1.info(f"Current time: {datetime.datetime.now().strftime('%H:%M:%S.%f')}")
        """
        We will use asyncio.sleep() to sleep for the interval, instead of time.sleep().
        This is because asyncio.sleep() is a coroutine, and will not block the event
        loop, allowing other concurrent functions to run. Because it is a coroutine,
        we will need to await it.
        """
        # Sleep for the interval.
        await asyncio.sleep(interval)

"""
We will create two more concurrent functions that are very similar to the first.
"""
# Function 2.
async def concurrent_function_2(interval=0.5):
    """
    A basic function that will print the time every interval seconds.
    """
    # We will use a while loop to keep the function running.
    while True:
        # Print the time.
        logger_2.info(f"Current time: {datetime.datetime.now().strftime('%H:%M:%S.%f')}")
        # Sleep for the interval.
        await asyncio.sleep(interval)

# Function 3.
async def concurrent_function_3(interval=0.5):
    """
    A basic function that will print the time every interval seconds.
    """
    # We will use a while loop to keep the function running.
    while True:
        # Print the time.
        logger_3.info(f"Current time: {datetime.datetime.now().strftime('%H:%M:%S.%f')}")
        # Sleep for the interval.
        await asyncio.sleep(interval)

"""
Running our concurrent functions.

Now that we have defined our concurrent functions, we will run them in our main function. This
function will be a coroutine itself and await the other concurrent functions. Later, in the
main call, this async main function will be run using asyncio.run().


To do this, we have to await the concurrent functions using 'await'. The program will wait 
for the concurrent functions to finish before moving on to the next line of code. To collect 
this set of concurrent functions, we will use asyncio.gather().
"""
async def main():
    """
    The main coroutine, just awaits our concurrent functions.
    """
    await asyncio.gather(
        concurrent_function_1(0.25),
        concurrent_function_2(0.5),
        concurrent_function_3(0.75)
    )

    
"""
Finally, we will run our program. In this example, we will run these concurrent functions
infinitely, gracefully exiting when the user presses Ctrl+C, which will trigger 
a KeyboardInterrupt.
"""
# Run the main function.
if __name__ == "__main__":
    # We will use a try/except block to catch the KeyboardInterrupt.
    try:
        """
        Once we have defined our main coroutine, we will run it using asyncio.run().
        """
        asyncio.run(main())
    except KeyboardInterrupt:
        """
        If the user presses Ctrl+C, we will gracefully exit the program.
        """
        print("Exiting program...")
        exit(0)

"""
This is just a simple example of using concurrent processing and threading in Python,
using asyncio. There are many more features of asyncio that can be used in your 
projects, so please check out the resources provided above. Happy coding!
"""