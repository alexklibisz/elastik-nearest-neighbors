/*
 * Copyright [2018] [Alex Klibisz]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.elasticsearch.plugin.aknn;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

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
    private List<RealMatrix> normalsTransposed;
    private List<RealVector> thresholds;

    public LshModel(Integer nbTables, Integer nbBitsPerTable, Integer nbDimensions, String description) {
        this.nbTables = nbTables;
        this.nbBitsPerTable = nbBitsPerTable;
        this.nbDimensions = nbDimensions;
        this.description = description;
        this.midpoints = new ArrayList<>();
        this.normals = new ArrayList<>();
        this.normalsTransposed = new ArrayList<>();
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

    public Map<String, Integer> getVectorHashes(List<Double> vector) {

        RealMatrix xDotNT, vectorAsMatrix;
        RealVector threshold;
        Map<String, Integer> hashes = new HashMap<>();
        Integer hash, i, j;

        // Have to convert the vector to a matrix to support multiplication below.
        // TODO: if the List<Double> vector argument can be changed to an Array double[] or float[], this would be faster.
        vectorAsMatrix = MatrixUtils.createRealMatrix(1, nbDimensions);
        for (i = 0; i < nbDimensions; i++)
            vectorAsMatrix.setEntry(0, i, vector.get(i));

        // Compute the hash for this vector with respect to each table.
        for (i = 0; i < nbTables; i++) {
            xDotNT = vectorAsMatrix.multiply(normalsTransposed.get(i));
            threshold = thresholds.get(i);
            hash = 0;
            for (j = 0; j < nbBitsPerTable; j++)
                if (xDotNT.getEntry(0, j) > threshold.getEntry(j))
                    hash += (int) Math.pow(2, j);
            hashes.put(i.toString(), hash);
        }

        return hashes;
    }

    @SuppressWarnings("unchecked")
    public static LshModel fromMap(Map<String, Object> serialized) {

        LshModel lshModel = new LshModel(
                (Integer) serialized.get("_aknn_nb_tables"), (Integer) serialized.get("_aknn_nb_bits_per_table"),
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
            lshModel.normalsTransposed.add(normal.transpose());
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
            put("_aknn_nb_bits_per_table", nbBitsPerTable);
            put("_aknn_nb_dimensions", nbDimensions);
            put("_aknn_description", description);
            put("_aknn_midpoints", midpoints.stream().map(realMatrix -> realMatrix.getData()).collect(Collectors.toList()));
            put("_aknn_normals", normals.stream().map(normals -> normals.getData()).collect(Collectors.toList()));
        }};
    }
}
