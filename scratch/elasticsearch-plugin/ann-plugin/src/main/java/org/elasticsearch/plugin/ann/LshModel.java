package org.elasticsearch.plugin.ann;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.json.JSONArray;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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

    }

    public Map<String, Object> getSerializable() {
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
