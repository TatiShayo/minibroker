import math
from typing import Dict, List, Optional

def assign_range(partitions: List[int], consumers: List[str]) -> Dict[str, List[int]]:
    """Range assignor: assigns contiguous segments of partitions to sorted consumers."""
    if not consumers:
        return {}
    if not partitions:
        return {c: [] for c in consumers}

    sorted_consumers = sorted(consumers)
    sorted_partitions = sorted(partitions)
    
    num_partitions = len(sorted_partitions)
    num_consumers = len(sorted_consumers)
    
    min_partitions_per_consumer = num_partitions // num_consumers
    extra_partitions = num_partitions % num_consumers
    
    assignment: Dict[str, List[int]] = {}
    current_idx = 0
    
    for i, consumer_id in enumerate(sorted_consumers):
        count = min_partitions_per_consumer + (1 if i < extra_partitions else 0)
        assignment[consumer_id] = sorted_partitions[current_idx:current_idx + count]
        current_idx += count
        
    return assignment

def assign_round_robin(partitions: List[int], consumers: List[str]) -> Dict[str, List[int]]:
    """Round-robin assignor: distributes partitions evenly one-by-one to consumers."""
    if not consumers:
        return {}
    if not partitions:
        return {c: [] for c in consumers}

    sorted_consumers = sorted(consumers)
    sorted_partitions = sorted(partitions)
    
    assignment: Dict[str, List[int]] = {c: [] for c in sorted_consumers}
    num_consumers = len(sorted_consumers)
    
    for i, p in enumerate(sorted_partitions):
        consumer_id = sorted_consumers[i % num_consumers]
        assignment[consumer_id].append(p)
        
    return assignment

def assign_sticky(
    partitions: List[int],
    consumers: List[str],
    current_assignment: Optional[Dict[str, List[int]]] = None
) -> Dict[str, List[int]]:
    """
    Sticky assignor: preserves existing partition assignments as much as possible,
    reassigning unassigned or abandoned partitions evenly.
    """
    if not consumers:
        return {}
    if not partitions:
        return {c: [] for c in consumers}

    active_consumers = set(consumers)
    all_partitions = set(partitions)
    target_count = math.ceil(len(partitions) / len(consumers))
    
    new_assignment: Dict[str, List[int]] = {c: [] for c in consumers}
    assigned_partitions = set()

    # Retain valid existing assignments up to target capacity
    if current_assignment:
        for c, parts in current_assignment.items():
            if c in active_consumers:
                valid_parts = [p for p in parts if p in all_partitions and p not in assigned_partitions]
                to_keep = valid_parts[:target_count]
                new_assignment[c].extend(to_keep)
                assigned_partitions.update(to_keep)

    # Find unassigned partitions
    unassigned = sorted(list(all_partitions - assigned_partitions))
    
    # Assign unassigned partitions to consumers with least load
    for p in unassigned:
        least_loaded = min(new_assignment.keys(), key=lambda c: (len(new_assignment[c]), c))
        new_assignment[least_loaded].append(p)

    return new_assignment

def assign_partitions(
    strategy: str,
    partitions: List[int],
    consumers: List[str],
    current_assignment: Optional[Dict[str, List[int]]] = None
) -> Dict[str, List[int]]:
    """Dispatch partition assignment based on selected strategy."""
    strat = (strategy or "range").lower().replace("-", "_")
    if strat == "round_robin":
        return assign_round_robin(partitions, consumers)
    elif strat == "sticky":
        return assign_sticky(partitions, consumers, current_assignment)
    else:
        return assign_range(partitions, consumers)
