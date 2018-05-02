package org.elasticsearch.plugin.ann;

import com.vividsolutions.jts.math.Matrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.json.JSONArray;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

public class LshModel {

    private Integer nbTables;
    private Integer nbBitsPerTable;
    private Integer nbDimensions;
    private String description;
    private List<RealMatrix> midpoints;
    private List<RealMatrix> normals;

    public LshModel(Integer nbTables, Integer nbBitsPerTable, Integer nbDimensions, String description) {
        this.nbTables = nbTables;
        this.nbBitsPerTable = nbBitsPerTable;
        this.nbDimensions = nbDimensions;
        this.description = description;
        this.midpoints = new ArrayList<>();
        this.normals = new ArrayList<>();
    }

    public void fitFromVectorSample(RealMatrix vectorSample) throws IOException {

        RealMatrix vectorsA, vectorsB, midpoint, normal;

        for (int i = 0; i < vectorSample.getRowDimension(); i += (nbBitsPerTable * 2)) {
            // Select two subsets of nbBitsPerTable vectors.
            vectorsA = vectorSample.getSubMatrix(i, i + nbBitsPerTable - 1, 0, nbDimensions - 1);
            vectorsB = vectorSample.getSubMatrix(i + nbBitsPerTable, i + 2 * nbBitsPerTable - 1, 0, nbDimensions - 1);

            // Compute the midpoint between each pair of vectors.
            midpoint = vectorsA.add(vectorsB).scalarMultiply(0.5);
            midpoints.add(midpoint);

            // Compute the normal vectors for each pair of vectors.
            normal = vectorsB.subtract(midpoint);
            normals.add(normal);
        }

    }

    public List<Integer> getVectorHashes(List<Double> vector) {
//        XdotN = X.dot(self.N.T)
//        H = (XdotN >= self.NdotM).astype(np.uint8)
//        return H
//        Integer[] hashes = new Integer[this.nbTables];

        RealMatrix vectorAsMatrix = MatrixUtils.createRealMatrix(1, nbDimensions);
        for (int i = 0; i < nbDimensions; i++)
            vectorAsMatrix.setEntry(0, i, vector.get(i));

        // Compute the hash for each table.
        for (int i = 0; i < nbTables; i++) {
            RealMatrix normal = normals.get(i);
            RealMatrix midpoint = midpoints.get(i);

            RealVector thresholds = new ArrayRealVector(this.nbBitsPerTable);
            System.out.println(thresholds.toString());
            for (int j = 0; j < this.nbBitsPerTable; j++) {
                thresholds.setEntry(j, normal.getRowVector(j).dotProduct(midpoint.getRowVector(j)));
            }

            RealMatrix xDotNT = vectorAsMatrix.multiply(normal.transpose());
            System.out.println(xDotNT.toString());
            System.out.println(thresholds.toString());
            System.out.println(String.format("%d %d %d", i, xDotNT.getRowDimension(), xDotNT.getColumnDimension()));

            double[] hashes = new double[nbBitsPerTable];
            for (int j = 0; j < nbBitsPerTable; j++) {
                if (xDotNT.getEntry(0, j) >= thresholds.getEntry(j))
                    hashes[j] = 1.0;
                else
                    hashes[j] = 0.0;
            }

            System.out.println(MatrixUtils.createRealVector(hashes).toString());
            System.out.println(">>>");

        }

        // Convert vector to RealVector.
//        double[] vectorData = new double[this.nbDimensions];
//        for (int i = 0; i < this.nbDimensions; i++)
//            vectorData[i] = vector.get(i);
//        final RealVector realVector = MatrixUtils.createRealVector(vectorData);
//
//        System.out.println(">>>");
//        System.out.println(realVector);
//        System.out.println(">>>");
//
//        for (int i = 0; i < this.nbTables; i++) {
//            double[] dotProducts = new double[this.nbBitsPerTable];
//            double s = 0.;
//            for (int j = 0; j < this.nbBitsPerTable; j++) {
//                dotProducts[j] = realVector.dotProduct(normals.get(i).getRowVector(j));
//                s += dotProducts[j];
//            }
//            System.out.println(String.format("%d %.4f", i, s));
//        }
//
//        System.out.println(">>>");

        return Arrays.asList(0,1,0);
    }

    @SuppressWarnings("unchecked")
    public static LshModel fromMap(Map<String, Object> serialized) {

        LshModel lshModel = new LshModel(
                (Integer) serialized.get("nbTables"), (Integer) serialized.get("nbBitsPerTable"),
                (Integer) serialized.get("nbDimensions"), (String) serialized.get("description"));

        // TODO: figure out how to cast directly to List<double[][]> or double[][][] and use MatrixUtils.createRealMatrix.
        List<List<List<Double>>> midpointsRaw = (List<List<List<Double>>>) serialized.get("midpoints");
        List<List<List<Double>>> normalsRaw = (List<List<List<Double>>>) serialized.get("normals");
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
        return lshModel;
    }

    public Map<String, Object> toMap() {
        return new HashMap<String, Object>() {{
            put("nbTables", nbTables);
            put("nbBitsPerTable", nbBitsPerTable);
            put("nbDimensions", nbDimensions);
            put("description", description);
            put("midpoints", midpoints.stream().map(realMatrix -> realMatrix.getData()).collect(Collectors.toList()));
            put("normals", normals.stream().map(normals -> normals.getData()).collect(Collectors.toList()));
        }};
    }
}
