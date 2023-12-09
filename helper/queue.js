export function createQueue() {
    return {
        head: null,
        tail: null,
        length: 0,
    }
}

export function enqueue(queue, value) {
    const node = {
        value: value,
        next: null,
    };
    if (queue.head === null) {
        queue.head = node;
    } else {
        queue.tail.next = node;
    }
    queue.tail = node;
    queue.length++;
}

export function dequeue(queue) {
    if (queue.head === null) {
        return null;
    }
    const node = queue.head;
    queue.head = node.next;
    if (queue.head === null) {
        queue.tail = null;
    }
    queue.length--;
    return node.value;
}