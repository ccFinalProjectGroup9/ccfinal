
public class Driver {
	public static void main(String[] args) throws Exception {

		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		MissingDataGenerator missingDataGenerator = new MissingDataGenerator();
		Normalize normalize = new Normalize();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();
		Recommendation recommendation = new Recommendation();

		String rawInput = args[0];
		String userBusinessListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String missingDataDir = args[3];
		String normalizeDir = args[4];
		String multiplicationDir = args[5];
		String sumDir = args[6];
		String recommendationDir = args[7];

		String[] path1 = {rawInput, userBusinessListOutputDir};
		String[] path2 = {userBusinessListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {rawInput, missingDataDir};
		String[] path4 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path5 = {normalizeDir, missingDataDir, multiplicationDir};
		String[] path6 = {multiplicationDir, sumDir};
		String[] path7 = {sumDir, rawInput, recommendationDir};

		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		missingDataGenerator.main(path3);
		normalize.main(path4);
		multiplication.main(path5);
		sum.main(path6);
		recommendation.main(path7);
	}

}
