import heapq
import argparse

def process_file(filename, n=10):
    """
    Reads a file and finds the top N keys with the highest counts.
    Each line in the file should be in the format 'key: count'.
    """
    min_heap = []  # Min-heap to keep the top N elements

    # Open and read the file
    with open(filename, 'r') as file:
        for line in file:
            # Parse the line into key and count
            parts = line.strip().split(":")
            if len(parts) != 2:
                continue  # Skip malformed lines
            
            key = parts[0].strip()
            try:
                count = int(parts[1].strip())
            except ValueError:
                continue  # Skip lines with invalid counts

            # Push to the heap
            heapq.heappush(min_heap, (count, key))

            # If the heap exceeds N elements, remove the smallest
            if len(min_heap) > n:
                heapq.heappop(min_heap)

    # Extract the top N elements from the heap and sort them in descending order
    top_n = sorted(min_heap, key=lambda x: -x[0])

    return top_n


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Find the top N keys with the highest counts from a file.")
    parser.add_argument("filename", type=str, help="Path to the input file")
    parser.add_argument("-n", type=int, default=10, help="Number of top elements to find (default: 10)")
    args = parser.parse_args()

    # Process the file
    top_n = process_file(args.filename, n=args.n)

    # Print the results
    print("Top Results:")
    for count, key in top_n:
        print(f"{key}: {count}")


if __name__ == "__main__":
    main()
