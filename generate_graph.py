
import matplotlib.pyplot as plt

def generate_graph(file_path):
    insert_times = []
    retrieve_times = []
    delete_times = []

    with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if line.startswith("Insert"):
                insert_times.append(float(line.split(": ")[1]))
            elif line.startswith("Retrieve"):
                retrieve_times.append(float(line.split(": ")[1]))
            elif line.startswith("Delete"):
                delete_times.append(float(line.split(": ")[1]))

    plt.plot(insert_times, label='Insert')
    plt.plot(retrieve_times, label='Retrieve')
    plt.plot(delete_times, label='Delete')
    plt.xlabel('Operation Count')
    plt.ylabel('Time (seconds)')
    plt.legend()
    plt.title('Performance Test')
    plt.savefig('performance_test.png')  # Save the graph to a file
    plt.show()

if __name__ == "__main__":
    generate_graph('performance_times.txt')