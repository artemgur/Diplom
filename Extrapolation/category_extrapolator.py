from collections import defaultdict


class CategoryExtrapolator:
    def __init__(self, seconds_between_snapshots):
        self._change_counts = defaultdict(lambda: defaultdict(int))
        self._no_change_counts = defaultdict(int)
        self._seconds_between_snapshots = seconds_between_snapshots  # TODO Variable time between snapshots

    def fit(self, data_history):
        for i in range(len(data_history) - 1):
            item = data_history[i]
            item_next = data_history[i + 1]
            self.add_element(item, item_next)

        return self
        #change_counts_sum = {k: sum(v.values()) for k, v in self._change_counts.items()}

    def add_element(self, old_newest, newest):
        if old_newest != newest:
            self._change_counts[old_newest][newest] += 1
        else:
            self._no_change_counts[old_newest] += 1

    def remove_element(self, old_oldest, oldest):
        if old_oldest != oldest:
            self._change_counts[old_oldest][oldest] -= 1
        else:
            self._no_change_counts[old_oldest] -= 1

    def extrapolate(self, newest, seconds_before_extrapolation):
        change_counts_sum = sum(self._change_counts[newest].values())
        sum_total = change_counts_sum + self._no_change_counts[newest]
        no_change_probability = self._no_change_counts[newest] / sum_total
        max_change_pair = max(self._change_counts[newest].items(), key=lambda x: x[1])
        max_change_probability = max_change_pair[1] / sum_total
        max_change_probability_time_scaled = max_change_probability * seconds_before_extrapolation / self._seconds_between_snapshots
        return max_change_pair[1] if max_change_probability_time_scaled < no_change_probability else newest

