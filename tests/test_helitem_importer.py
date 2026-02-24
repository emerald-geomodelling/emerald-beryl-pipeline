"""Tests for HeliTEM2LibAarhusImporter."""

import os
import tempfile
import unittest

import libaarhusxyz

import beryl_pipeline.file_import


datadir = os.path.join(os.path.dirname(__file__), "data", "helitem")
ASC_FILE = os.path.join(datadir, "test_helitem.asc")
GEX_FILE = os.path.join(datadir, "test_helitem.gex")

PROJECTION = 32611  # UTM Zone 11N
SCALEFACTOR = 1e-12


def _has_test_data():
    return os.path.exists(ASC_FILE) and os.path.exists(GEX_FILE)


@unittest.skipUnless(_has_test_data(), "HeliTEM test data not available")
class TestHeliTEM2LibAarhusImporter(unittest.TestCase):
    """Test the HeliTEM2LibAarhus importer class."""

    @classmethod
    def setUpClass(cls):
        """Create the importer once for all tests (conversion is slow)."""
        cls.importer = beryl_pipeline.file_import.HeliTEM2LibAarhusImporter(
            files={"ascfile": ASC_FILE, "gexfile": GEX_FILE},
            scalefactor=SCALEFACTOR,
            projection=PROJECTION,
        )

    def test_is_survey(self):
        """Importer extends libaarhusxyz.Survey."""
        self.assertIsInstance(self.importer, libaarhusxyz.Survey)

    def test_has_xyz(self):
        """Importer has a valid XYZ object."""
        self.assertIsNotNone(self.importer.xyz)
        self.assertIsInstance(self.importer.xyz, libaarhusxyz.XYZ)

    def test_has_gex(self):
        """Importer has a valid GEX object."""
        self.assertIsNotNone(self.importer.gex)
        self.assertIsInstance(self.importer.gex, libaarhusxyz.GEX)

    def test_sounding_count(self):
        """Should have 6 soundings from the test fixture."""
        self.assertEqual(len(self.importer.xyz.flightlines), 6)

    def test_flight_lines(self):
        """Should have 2 flight lines from the test fixture."""
        lines = self.importer.xyz.split_by_line()
        self.assertEqual(len(lines), 2)

    def test_model_info_projection(self):
        """Projection should be set in model_info."""
        self.assertEqual(self.importer.xyz.model_info['projection'], PROJECTION)

    def test_model_info_scalefactor(self):
        """Scalefactor should be set in model_info."""
        self.assertAlmostEqual(self.importer.xyz.model_info['scalefactor'], SCALEFACTOR)

    def test_has_ch01_gate_data(self):
        """Should have Ch01 gate data (Z-component dB/dt)."""
        # With naming_standard="alc", gate data uses Aarhus naming convention
        self.assertIn('Gate_Ch01', self.importer.xyz.layer_data)
        # 25 gates for HeliTEM
        self.assertEqual(self.importer.xyz.layer_data['Gate_Ch01'].shape[1], 25)

    def test_gate_data_normalised(self):
        """Gate data should be normalised: stored = raw_nTs * 1e3 / DipoleMoment."""
        from emerald_helitem_converter.reader import read_helitem_asc, read_gex_metadata
        from emerald_helitem_converter.converter import to_aarhus_xyz
        from emerald_helitem_converter.writer import write_xyz
        import numpy as np

        # Read raw data independently through the converter
        data = read_helitem_asc(ASC_FILE)
        gex = read_gex_metadata(GEX_FILE)
        aarhus = to_aarhus_xyz(data, gex, projection=PROJECTION)

        # Write and reload to get normalised column names
        with tempfile.TemporaryDirectory() as tmpdir:
            xyz_path = os.path.join(tmpdir, "raw.xyz")
            write_xyz(aarhus, xyz_path)
            raw_xyz = libaarhusxyz.XYZ(xyz_path, normalize=True, naming_standard="alc")

        raw_gate = raw_xyz.layer_data['Gate_Ch01']
        raw_dipole = raw_xyz.flightlines['DipoleMoment_Ch01']

        # The importer's stored values
        stored_gate = self.importer.xyz.layer_data['Gate_Ch01']

        # Verify normalisation: stored ~= raw * 1e3 / DipoleMoment
        for row_idx in range(min(3, len(raw_gate))):
            for col_idx in range(min(5, raw_gate.shape[1])):
                raw_val = raw_gate.iloc[row_idx, col_idx]
                dipole_val = raw_dipole.iloc[row_idx]
                stored_val = stored_gate.iloc[row_idx, col_idx]
                if np.isnan(raw_val) or np.isnan(dipole_val) or dipole_val == 0:
                    continue
                expected = raw_val * 1e3 / dipole_val
                self.assertAlmostEqual(stored_val, expected, places=3,
                    msg=f"Row {row_idx}, gate {col_idx}: {stored_val} != {expected}")

    def test_std_data_unchanged(self):
        """STD_Ch01 should be unchanged (relative fractions, unit-independent)."""
        if 'STD_Ch01' not in self.importer.xyz.layer_data:
            self.skipTest("No STD_Ch01 in test data")
        std_data = self.importer.xyz.layer_data['STD_Ch01']
        import numpy as np
        valid = std_data.values[~np.isnan(std_data.values)]
        if len(valid) > 0:
            # Relative STD should be small positive values
            self.assertTrue((valid >= 0).all(), "STD values should be non-negative")
            self.assertTrue((valid < 10).all(), "STD values should be reasonable fractions")

    def test_scalefactor_is_1e_minus_12(self):
        """After normalisation, scalefactor must be 1e-12."""
        self.assertAlmostEqual(self.importer.xyz.model_info['scalefactor'], 1e-12)

    def test_dump_produces_outputs(self):
        """dump() should produce valid output files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self.importer.dump(
                xyzfile=os.path.join(tmpdir, 'out.xyz'),
                gexfile=os.path.join(tmpdir, 'out.gex'),
                msgpackfile=os.path.join(tmpdir, 'out.msgpack'),
                summaryfile=os.path.join(tmpdir, 'out.summary.yml'),
                geojsonfile=os.path.join(tmpdir, 'out.geojson'),
            )
            self.assertTrue(os.path.exists(os.path.join(tmpdir, 'out.xyz')))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, 'out.gex')))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, 'out.msgpack')))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, 'out.summary.yml')))
            self.assertTrue(os.path.exists(os.path.join(tmpdir, 'out.geojson')))

    def test_dump_xyz_loadable(self):
        """Output .xyz from dump() should be loadable by libaarhusxyz."""
        with tempfile.TemporaryDirectory() as tmpdir:
            xyz_path = os.path.join(tmpdir, 'out.xyz')
            self.importer.dump(
                xyzfile=xyz_path,
                gexfile=os.path.join(tmpdir, 'out.gex'),
                msgpackfile=os.path.join(tmpdir, 'out.msgpack'),
                summaryfile=os.path.join(tmpdir, 'out.summary.yml'),
                geojsonfile=os.path.join(tmpdir, 'out.geojson'),
            )
            reloaded = libaarhusxyz.XYZ(xyz_path)
            self.assertEqual(len(reloaded.flightlines), 6)

    def test_split_by_line_dump(self):
        """Per-line dump should work (same pattern as Import.run)."""
        import copy
        with tempfile.TemporaryDirectory() as tmpdir:
            line_count = 0
            for fline, line_data in self.importer.xyz.split_by_line().items():
                line_importer = copy.copy(self.importer)
                line_importer.xyz = line_data
                line_importer.dump(
                    xyzfile=os.path.join(tmpdir, f'out.{fline}.xyz'),
                    gexfile=os.path.join(tmpdir, f'out.{fline}.gex'),
                    msgpackfile=os.path.join(tmpdir, f'out.{fline}.msgpack'),
                    summaryfile=os.path.join(tmpdir, f'out.{fline}.summary.yml'),
                    geojsonfile=os.path.join(tmpdir, f'out.{fline}.geojson'),
                )
                self.assertTrue(os.path.exists(os.path.join(tmpdir, f'out.{fline}.xyz')))
                line_count += 1
            self.assertEqual(line_count, 2)


@unittest.skipUnless(_has_test_data(), "HeliTEM test data not available")
class TestHeliTEM2LibAarhusImporterErrors(unittest.TestCase):
    """Test error handling."""

    def test_missing_asc_file(self):
        """Should raise AssertionError if .asc file is missing."""
        with self.assertRaises(AssertionError):
            beryl_pipeline.file_import.HeliTEM2LibAarhusImporter(
                files={"gexfile": GEX_FILE},
                scalefactor=SCALEFACTOR,
                projection=PROJECTION,
            )

    def test_missing_gex_file(self):
        """Should raise AssertionError if .gex file is missing."""
        with self.assertRaises(AssertionError):
            beryl_pipeline.file_import.HeliTEM2LibAarhusImporter(
                files={"ascfile": ASC_FILE},
                scalefactor=SCALEFACTOR,
                projection=PROJECTION,
            )

    def test_invalid_projection(self):
        """Should raise AssertionError for invalid projection."""
        with self.assertRaises(AssertionError):
            beryl_pipeline.file_import.HeliTEM2LibAarhusImporter(
                files={"ascfile": ASC_FILE, "gexfile": GEX_FILE},
                scalefactor=SCALEFACTOR,
                projection=None,
            )

    def test_zero_scalefactor(self):
        """Should raise AssertionError for zero scalefactor."""
        with self.assertRaises(AssertionError):
            beryl_pipeline.file_import.HeliTEM2LibAarhusImporter(
                files={"ascfile": ASC_FILE, "gexfile": GEX_FILE},
                scalefactor=0.0,
                projection=PROJECTION,
            )


class TestHeliTEMEntryPoint(unittest.TestCase):
    """Test entry point registration."""

    def test_entry_point_registered(self):
        """HeliTEM2LibAarhus should be discoverable via entry points."""
        self.assertIn('HeliTEM2LibAarhus', beryl_pipeline.file_import.importers)

    def test_entry_point_loads_correct_class(self):
        """Entry point should load HeliTEM2LibAarhusImporter."""
        cls = beryl_pipeline.file_import.importers['HeliTEM2LibAarhus'].load()
        self.assertIs(cls, beryl_pipeline.file_import.HeliTEM2LibAarhusImporter)

    def test_skytem_entry_point_still_works(self):
        """Existing SkyTEM entry point should still be registered."""
        self.assertIn('SkyTEM XYZ', beryl_pipeline.file_import.importers)


if __name__ == '__main__':
    unittest.main()
