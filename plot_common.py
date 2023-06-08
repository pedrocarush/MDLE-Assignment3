from datetime import datetime
from typing import List, Tuple
import matplotlib.pyplot as plt



def plot_ones_over_time(ones_at_each_timestamp: List[Tuple[datetime, int]], title_suffix: str = '', save_no_show: bool = False):
    
    timestamps = [t[0] for t in ones_at_each_timestamp]
    ones = [t[1] for t in ones_at_each_timestamp]

    plt.scatter(timestamps, ones, s=2.5)
    plt.title('Total 1s in the last 1000 bits' + title_suffix)
    plt.xlabel('Timestamp')
    plt.ylabel('Number of 1s')

    if save_no_show:
        plt.savefig('results/dgim_ones.png')
    else:
        plt.show()


def plot_top_5_over_time(top_5_at_each_timestamp: List[Tuple[datetime, List[str]]], title_suffix: str = '', save_no_show: bool = False):

    timestamps = [t[0] for t in top_5_at_each_timestamp]
    top_5s = [t[1] for t in top_5_at_each_timestamp]

    # Get all items seen in stream
    all_items = {item for top5 in top_5s for item in top5}

    # We do this loop to plot, at each scatter() call, all points that correspond to a given item, so that they are colored equally
    for item_to_plot in all_items:
        points_to_plot_x = []
        points_to_plot_y = []
        for timestamp, top_5 in zip(timestamps, top_5s):
            for rank, item in enumerate(top_5):
                if item == item_to_plot:
                    points_to_plot_x.append(timestamp)
                    points_to_plot_y.append(rank + 1)
                    break
        
        plt.scatter(points_to_plot_x, points_to_plot_y, label=item_to_plot)

    plt.title('Top 5 items over time' + title_suffix)
    plt.xlabel('Timestamp')
    plt.ylabel('Rank (top place)')
    plt.legend(bbox_to_anchor=(1.04, 1), borderaxespad=0)
    
    plt.tight_layout()
    if save_no_show:
        plt.savefig('results/edw_top.png')
    else:
        plt.show()