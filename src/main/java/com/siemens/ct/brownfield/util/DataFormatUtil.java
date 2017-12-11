package com.siemens.ct.brownfield.util;

/**
 * @Author He Bao Jing Z003NUJF
 * @Date Nov 17, 2017
 *
 */

public class DataFormatUtil {

	public static double[] Str2DoubleArray(String str, String token) {
		final String[] strArray = str.split(token);
		final double[] doubleArray = new double[strArray.length];

		int i = 0;
		for (String s : strArray) {
			if (s != null ) {
				doubleArray[i++] = Double.valueOf(s);
			}
		}
		return doubleArray;
	}

	public static String FloatArray2Str(final double[] data, String token) {

		String str = new String();

		for (int i = 0; i < data.length; ++i) {
			
			str += String.valueOf((float) data[i]);
			
			if (i != data.length - 1) {
				str += token;
			}
		}

		return str;
	}

	// public static double[] Str2DoubleArray(String str, String token) {
	// final String[] strArray = str.split(token);
	// final double[] doubleArray = new double[strArray.length];
	//
	// int i = 0;
	// for (String s : strArray) {
	// doubleArray[i++] = Double.valueOf(s);
	// }
	// return doubleArray;
	// }
	//
	// public static String DoubleArray2Str(final double[] data, String token) {
	//
	// String str = new String();
	//
	// for (int i = 0; i < data.length; ++i) {
	// str += String.valueOf(data[i]);
	//
	// if (i != data.length - 1) {
	// str += token;
	// }
	// }
	//
	// return str;
	// }

}
