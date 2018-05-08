package org.elasticsearch.plugin.aknn;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LshModel {

    private Integer nbTables;
    private Integer nbBitsPerTable;
    private Integer nbDimensions;
    private String description;
    private List<RealMatrix> midpoints;
    private List<RealMatrix> normals;
    private List<RealVector> thresholds;

    public LshModel(Integer nbTables, Integer nbBitsPerTable, Integer nbDimensions, String description) {
        this.nbTables = nbTables;
        this.nbBitsPerTable = nbBitsPerTable;
        this.nbDimensions = nbDimensions;
        this.description = description;
        this.midpoints = new ArrayList<>();
        this.normals = new ArrayList<>();
        this.thresholds = new ArrayList<>();
    }

    public void fitFromVectorSample(List<List<Double>> vectorSample) {

        RealMatrix vectorsA, vectorsB, midpoint, normal, vectorSampleMatrix;
        vectorSampleMatrix = MatrixUtils.createRealMatrix(vectorSample.size(), this.nbDimensions);

        for (int i = 0; i < vectorSample.size(); i++)
            for (int j = 0; j < this.nbDimensions; j++)
                vectorSampleMatrix.setEntry(i, j, vectorSample.get(i).get(j));


        for (int i = 0; i < vectorSampleMatrix.getRowDimension(); i += (nbBitsPerTable * 2)) {
            // Select two subsets of nbBitsPerTable vectors.
            vectorsA = vectorSampleMatrix.getSubMatrix(i, i + nbBitsPerTable - 1, 0, nbDimensions - 1);
            vectorsB = vectorSampleMatrix.getSubMatrix(i + nbBitsPerTable, i + 2 * nbBitsPerTable - 1, 0, nbDimensions - 1);

            // Compute the midpoint between each pair of vectors.
            midpoint = vectorsA.add(vectorsB).scalarMultiply(0.5);
            midpoints.add(midpoint);

            // Compute the normal vectors for each pair of vectors.
            normal = vectorsB.subtract(midpoint);
            normals.add(normal);
        }

    }

    public List<Long> getVectorHashes(List<Double> vector) {

        List<Long> hashes = new ArrayList<>();

//        long timestamp = System.nanoTime();
        RealMatrix vectorAsMatrix = MatrixUtils.createRealMatrix(1, nbDimensions);
        for (int i = 0; i < nbDimensions; i++)
            vectorAsMatrix.setEntry(0, i, vector.get(i));
//        System.out.println(String.format("   %d", System.nanoTime() - timestamp));

//        timestamp = System.nanoTime();
        // Compute the hash for this vector with respect to each table.
        for (int i = 0; i < nbTables; i++) {
            RealMatrix normal = normals.get(i);
            RealVector threshold = thresholds.get(i);
            RealMatrix xDotNT = vectorAsMatrix.multiply(normal.transpose());
            Long hash = 0L;
            for (int j = 0; j < nbBitsPerTable; j++)
                if (xDotNT.getEntry(0, j) >= threshold.getEntry(j))
                    hash += (long) Math.pow(2, j);

            hashes.add(hash);
        }
//        System.out.println(String.format("   %d", System.nanoTime() - timestamp));

        return hashes;
    }

    @SuppressWarnings("unchecked")
    public static LshModel fromMap(Map<String, Object> serialized) {

        LshModel lshModel = new LshModel(
                (Integer) serialized.get("_aknn_nb_tables"), (Integer) serialized.get("_aknn_bits_per_table"),
                (Integer) serialized.get("_aknn_nb_dimensions"), (String) serialized.get("_aknn_description"));

        // TODO: figure out how to cast directly to List<double[][]> or double[][][] and use MatrixUtils.createRealMatrix.
        List<List<List<Double>>> midpointsRaw = (List<List<List<Double>>>) serialized.get("_aknn_midpoints");
        List<List<List<Double>>> normalsRaw = (List<List<List<Double>>>) serialized.get("_aknn_normals");
        for (int i = 0; i < lshModel.nbTables; i++) {
            RealMatrix midpoint = MatrixUtils.createRealMatrix(lshModel.nbBitsPerTable, lshModel.nbDimensions);
            RealMatrix normal = MatrixUtils.createRealMatrix(lshModel.nbBitsPerTable, lshModel.nbDimensions);
            for (int j = 0; j < lshModel.nbBitsPerTable; j++) {
                for (int k = 0; k < lshModel.nbDimensions; k++) {
                    midpoint.setEntry(j, k, midpointsRaw.get(i).get(j).get(k));
                    normal.setEntry(j, k, normalsRaw.get(i).get(j).get(k));
                }
            }
            lshModel.midpoints.add(midpoint);
            lshModel.normals.add(normal);
        }

        for (int i = 0; i < lshModel.nbTables; i++) {
            RealMatrix normal = lshModel.normals.get(i);
            RealMatrix midpoint = lshModel.midpoints.get(i);
            RealVector threshold = new ArrayRealVector(lshModel.nbBitsPerTable);
            for (int j = 0; j < lshModel.nbBitsPerTable; j++)
                threshold.setEntry(j, normal.getRowVector(j).dotProduct(midpoint.getRowVector(j)));
            lshModel.thresholds.add(threshold);
        }

        return lshModel;
    }

    public Map<String, Object> toMap() {
        return new HashMap<String, Object>() {{
            put("_aknn_nb_tables", nbTables);
            put("_aknn_bits_per_table", nbBitsPerTable);
            put("_aknn_nb_dimensions", nbDimensions);
            put("_aknn_description", description);
            put("_aknn_midpoints", midpoints.stream().map(realMatrix -> realMatrix.getData()).collect(Collectors.toList()));
            put("_aknn_normals", normals.stream().map(normals -> normals.getData()).collect(Collectors.toList()));
        }};
    }
}
