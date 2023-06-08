import gzip
import os.path as path
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from io import SEEK_CUR
from typing import List

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.ml.clustering import PowerIterationClustering
from pyspark.sql import SparkSession
from scipy.sparse.linalg import eigs
from sklearn.cluster import KMeans, SpectralClustering


def plot_clustering(graph: nx.Graph, clustering_labels: List[int], implementation: str, chosen_dataset: str):
    nx.draw(graph, node_color=clustering_labels, node_size=10, width = 0.1, cmap=plt.cm.jet)
    plt.savefig("results/"+implementation + "_" + chosen_dataset + ".png", format="PNG")
    plt.close()


def load_dataset(dataset: str, data_folder: str) -> pd.DataFrame:
    pt = path.join(data_folder, dataset)
    
    if dataset == 'ca-HepPh.txt.gz':
        with gzip.open(pt, 'rb') as f:
            c = f.read(1)
            while c == b'#':
                f.readline()
                c = f.read(1)
            f.seek(-1, SEEK_CUR)
            return pd.read_csv(f, sep='\t', header=None)
        
    elif dataset == 'facebook_combined.txt.gz':
        return pd.read_csv(pt, sep=' ', header=None)

    return pd.read_csv(pt, header=None)

def our_implementation(graph: nx.Graph, chosen_dataset: str, n_clusters: int, n_components: int):

    laplacian_matrix = nx.laplacian_matrix(graph).todense()

    eigenvalues, eigenvectors = eigs(laplacian_matrix.astype(np.float32), k=n_components, which='SR')

    #np.sort(eigenvalues)

    clustering = KMeans(n_clusters=n_clusters).fit(eigenvectors.real)

    return clustering.labels_

def sklearn_implementation(graph: nx.Graph, n_clusters: int):

    clustering = SpectralClustering(
        n_clusters=n_clusters,
        n_components=n_clusters,
        affinity='precomputed',
        assign_labels='kmeans',
        random_state=0
        ).fit(nx.adjacency_matrix(graph).todense())
    
    return clustering.labels_

def spark_implementation(dataset: pd.DataFrame, n_clusters: int):

    spark = SparkSession.builder \
        .appName('SpectralClustering') \
        .config('spark.master', 'local[*]') \
        .getOrCreate()

    dataset_spark = spark.createDataFrame(dataset)

    dataset_spark = dataset_spark.withColumn('weight', F.lit(1))

    pic = PowerIterationClustering(k=n_clusters, maxIter=60, initMode="random", srcCol='source', dstCol='target', weightCol="weight")
    dataset_clustered = pic.assignClusters(dataset_spark).toPandas().sort_values(by=['id'])

    return dataset_clustered['cluster'].values

def main(
        data_folder: str,
        chosen_dataset: str
):
    
    dataset = load_dataset(chosen_dataset,data_folder).rename(columns={0: 'source', 1: 'target'})

    # Obtained by manual analysis of the first few eigenvectors
    dataset_n_clusters = {
        'ca-HepPh.txt.gz': 15,
        'facebook_combined.txt.gz': 6,
        'PP-Pathways_ppi.csv.gz': 12,
    }

    dataset_n_components = {
        'ca-HepPh.txt.gz': 20,
        'facebook_combined.txt.gz': 8,
        'PP-Pathways_ppi.csv.gz': 12,
    }

    n_clusters = dataset_n_clusters[chosen_dataset]
    n_components = dataset_n_components[chosen_dataset]

    graph = nx.from_pandas_edgelist(dataset, source='source', target='target', create_using=nx.Graph)

    if chosen_dataset == 'ca-HepPh.txt.gz' or chosen_dataset == 'PP-Pathways_ppi.csv.gz':
        remove = [node for node,degree in dict(graph.degree()).items() if degree < 7]
        graph.remove_nodes_from(remove)

    our_labels = our_implementation(graph, chosen_dataset, n_clusters, n_components)

    plot_clustering(graph, our_labels, "our_implementation", chosen_dataset)

    sklearn_labels = sklearn_implementation(graph, n_clusters)
    
    plot_clustering(graph, sklearn_labels, "sklearn_implementation", chosen_dataset)

    spark_labels = spark_implementation(dataset, n_clusters)

    plot_clustering(graph, spark_labels, "spark_implementation", chosen_dataset)




if __name__ == '__main__':

    default_str = ' (default: %(default)s)'

    parser = ArgumentParser(
        prog="Spectral_Graph_Partitioning",
        formatter_class=RawDescriptionHelpFormatter,
        description="""Python-file version of Exercise 1's Jupyter notebook, for submission via spark-submit.
The documentation is present in the notebook.
The datasets 'ca-HepPh.txt.gz', 'facebook_combined.txt.gz' and 'PP-Pathways_ppi.csv.gz' all have to be zipped into the data path."""
    )
    parser.add_argument("--data-folder", type=str, help="path to the folder housing the dataset files" + default_str, default="./data")
    parser.add_argument("--chosen-dataset", type=str, help="name of the dataset to be used" + default_str, default="facebook_combined.txt.gz")

    args = parser.parse_args()

    main(
        data_folder=args.data_folder,
        chosen_dataset=args.chosen_dataset
    )