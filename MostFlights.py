import threading
from collections import defaultdict
import queue

with open('data/AComp_Passenger_data_no_error.csv', 'r') as file:
    data = file.readlines()

class Mapper(threading.Thread):

    def __init__(self, data_chunk, output_queue):
        threading.Thread.__init__(self)
        self.data_chunk = data_chunk
        self.output_queue = output_queue

    def run(self):
        # Map stage, counting the number of flights per passenger
        counts = defaultdict(int)
        for record in self.data_chunk:
            passenger_id = record[:10]
            counts[passenger_id] += 1

        # Put (Passenger ID, Flight Count) into the output queue as a key-value pair
        for passenger_id, count in counts.items():
            self.output_queue.put((passenger_id, count))

class Reducer(threading.Thread):

    def __init__(self, input_queue, output_queue):
        threading.Thread.__init__(self)
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self):
        counts = defaultdict(int)
        while True:
            try:
                passenger_id, count = self.input_queue.get(timeout=1)
                self.input_queue.task_done()
            except queue.Empty:
                break
            counts[passenger_id] += count

        for passenger_id, count in counts.items():
            self.output_queue.put((passenger_id, count))

def main():
    map_output_queue = queue.Queue()  # Storing the intermediate results of the Map phase
    reduce_output_queue = queue.Queue()  # Storing the intermediate results of the Reduce phase
    threads = []

    # Create Map thread and start it
    chunk_size = len(data) // 4  # Split the data into four chunks
    for i in range(0, len(data), chunk_size):
        thread = Mapper(data[i:i+chunk_size], map_output_queue)
        thread.start()
        threads.append(thread)

    # Waiting for all Map stage threads to finish executing
    for thread in threads:
        thread.join()

    # Create a thread for the Reduce phase and start it.
    reducer = Reducer(map_output_queue, reduce_output_queue)
    reducer.start()

    # Waiting for the threads in the Reduce phase to finish executing
    reducer.join()

    # Merge the results of the Reduce phase
    final_counts = defaultdict(int)
    while not reduce_output_queue.empty():
        passenger_id, count = reduce_output_queue.get()
        final_counts[passenger_id] += count

    # Find the travelers who make the most trips
    max_flights = max(final_counts.items(), key=lambda x: x[1])
    print(f"出行次数最多的旅客是 {max_flights[0]}，共出行 {max_flights[1]} 次。")

if __name__ == "__main__":
    main()
