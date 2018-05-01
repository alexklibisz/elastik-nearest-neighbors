package org.elasticsearch.plugin.ann;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.json.JSONArray;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LshModel {

    public Integer nbTables;
    public Integer nbBitsPerTable;
    public Integer nbDimensions;

    private List<RealMatrix> midpoints;
    private List<RealMatrix> normals;

    public LshModel(Integer nbTables, Integer nbBitsPerTable, Integer nbDimensions) {
        this.nbTables = nbTables;
        this.nbBitsPerTable = nbBitsPerTable;
        this.nbDimensions = nbDimensions;
    }

    public void fitFromVectorSample(RealMatrix vectorSample) throws IOException {

        RealMatrix vectorsA, vectorsB, midpoint, normal;
        midpoints = new ArrayList<>();
        normals = new ArrayList<>();

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

        double[][] tmp = midpoints.get(0).getData();
        JSONArray tmpJSON = new JSONArray(tmp);
        System.out.println(tmpJSON.toString());

//        midpoint = midpoints.get(0);
//        double[][] tmp = midpoint.getData();
//
//        System.out.println(midpoints.toString());
//        System.out.println(normals.toString());
//        System.out.println(">>>");
//        System.out.println(midpoints.get(0).getData().getClass());
//        System.out.println(">>>");

//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream(bos);
//        System.out.println(bos.size());
//        System.out.println(bos.toString());
//        MatrixUtils.serializeRealMatrix(midpoints.get(0), oos);
//        oos.flush();
//        oos.close();
//        System.out.println(bos.size());
//        System.out.println(bos.toString());

    }


}
