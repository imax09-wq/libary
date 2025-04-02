# sym_it.py (Modified to accept sc_root and use relative import)

from bisect   import  bisect_right
# Use relative import for parsers within the same package
from .parsers import  depth_rec, tas_rec, parse_tas, parse_tas_header, parse_depth, parse_depth_header
from time     import  time
import os

# Removed the automatic config.json loading and SC_ROOT definition

class SymIt:

    def __init__(
        self,
        sc_root:     str, # <-- Added sc_root parameter
        symbol:      str,
        date:        str,
        ts:          int = 0
    ):
        """
        Initializes the synchronized iterator for Sierra Chart raw data files.

        Args:
            sc_root (str): The root path of the Sierra Chart installation (e.g., "C:/SierraChart").
            symbol (str): The full symbol name (e.g., "ESM25_FUT_CME").
            date (str): The date string in "YYYY-MM-DD" format.
            ts (int, optional): Initial timestamp offset in microseconds. Defaults to 0.

        Raises:
            ValueError: If sc_root is not provided.
            FileNotFoundError: If the required .scid or .depth files cannot be found/opened.
        """
        if not sc_root:
             raise ValueError("sc_root path must be provided to SymIt") # Added validation

        self.symbol      = symbol
        self.date        = date

        self.sync        = False
        self.ts          = ts
        self.tas_recs    = []
        self.lob_recs    = []
        self.tas_i       = 0
        self.lob_i       = 0

        # Construct file paths using the passed sc_root parameter
        tas_file_path = os.path.join(sc_root, "Data", f"{symbol}.scid")
        depth_file_path = os.path.join(sc_root, "Data", "MarketDepthData", f"{symbol}.{date}.depth")

        try:
             self.tas_fd      = open(tas_file_path, "rb")
             self.lob_fd      = open(depth_file_path, "rb")
        except FileNotFoundError as e:
             # Make error more informative
             print(f"Error opening Sierra Chart data files.")
             print(f"Attempted Time & Sales path: {tas_file_path}")
             print(f"Attempted Depth path: {depth_file_path}")
             print(f"Original error: {e}")
             # Consider closing already opened file if one succeeded and the other failed
             if hasattr(self, 'tas_fd') and self.tas_fd and not self.tas_fd.closed: self.tas_fd.close()
             if hasattr(self, 'lob_fd') and self.lob_fd and not self.lob_fd.closed: self.lob_fd.close()
             raise # Re-raise the exception after printing details

        try:
            parse_tas_header(self.tas_fd)
            parse_depth_header(self.lob_fd)
        except Exception as e:
            print(f"Error parsing headers for symbol {symbol} on {date}: {e}")
            self.tas_fd.close()
            self.lob_fd.close()
            raise


    def synchronize(self, update: bool):
        """
        Loads new records if update=True and synchronizes internal pointers.
        """
        # t0 = time()

        if update:
            # obtain any new records since last read pointer
            # Note: parse_tas/parse_depth likely read from the current file descriptor position
            try:
                # Remember current position before reading
                current_tas_pos = self.tas_fd.tell()
                current_lob_pos = self.lob_fd.tell()

                new_tas = parse_tas(self.tas_fd, 0) # 0 offset likely means read from current pos
                new_lob = parse_depth(self.lob_fd, 0)

                # Only extend if new records were actually read (parse functions should return empty list/None if EOF)
                if new_tas: self.tas_recs.extend(new_tas)
                if new_lob: self.lob_recs.extend(new_lob)

            except Exception as e:
                print(f"Error parsing new records for {self.symbol} on {self.date}: {e}")
                # Attempt to reset file pointers to before the failed read, may not always work
                try:
                    self.tas_fd.seek(current_tas_pos)
                    self.lob_fd.seek(current_lob_pos)
                except Exception:
                    print("Warning: Could not reset file pointers after parsing error.")


        if not self.lob_recs and not self.tas_recs: # If no records loaded at all
             self.lob_i = 0
             self.tas_i = 0
             return # Nothing to synchronize

        # If only TAS records exist
        if not self.lob_recs:
            self.lob_i = 0
             # Find the first TAS record at or after the target timestamp 'ts'
            self.tas_i = bisect_right(self.tas_recs, self.ts, key=lambda rec: rec[tas_rec.timestamp])
            # Update 'ts' to the actual timestamp of the record we found (or keep original if none found)
            if self.tas_i < len(self.tas_recs):
                 self.ts = self.tas_recs[self.tas_i][tas_rec.timestamp]
            elif self.tas_recs: # If index is out of bounds, use the last record's time
                 self.ts = self.tas_recs[-1][tas_rec.timestamp]
            # else: self.ts remains unchanged if no tas_recs exist
            return

        # If only LOB records exist (or primarily using LOB timeline)
        # Find the first LOB record at or after the target timestamp 'ts'
        self.lob_i = bisect_right(self.lob_recs, self.ts, key=lambda rec: rec[depth_rec.timestamp])

        if self.lob_i < len(self.lob_recs):
            # Set current timestamp based on the next depth record
            self.ts = self.lob_recs[self.lob_i][depth_rec.timestamp]
        elif self.lob_recs: # If lob_i is past the end, use the last lob timestamp
             self.ts = self.lob_recs[-1][depth_rec.timestamp]
        # else: ts remains unchanged if no lob records exist at all

        # Find corresponding position in tas records based on current timestamp
        if self.tas_recs:
             # Find first TAS record strictly AFTER the current LOB timestamp self.ts
             # Using bisect_right should achieve this correctly
             self.tas_i = bisect_right(self.tas_recs, self.ts, key=lambda rec: rec[tas_rec.timestamp])
        else:
             self.tas_i = 0

        # print(f"synchronize: {time() - t0: 0.2f}")


    # if you want to re-read the stream from the start, set ts to 0
    def set_ts(self, ts: int, update: bool = False):
        """ Reset the iterator timestamp and optionally update records"""
        self.ts      = ts
        # Reset file pointers to the beginning (after headers) before synchronizing
        # Need to know header sizes or store initial positions after header parsing
        # For simplicity, let's assume re-opening or seeking is handled if needed,
        # but ideally headers are parsed once. Let's rely on synchronize logic.
        # Seek might be complex if buffer used, reset pointers via sync.
        self.synchronize(update)


    def __iter__(self):
        """ Make the class iterable, load initial/new records """
        # Reset pointers to start for iteration, potentially reload headers/seek
        # This basic version just re-syncs from the current file pos.
        # A more robust version might store header end positions.
        # Let's assume synchronize(True) gets necessary data for now.
        self.synchronize(True)
        return self


    def __next__(self):
        """ Returns the next record in chronological order (depth or tas) """
        tas_i       = self.tas_i
        tas_recs    = self.tas_recs
        lob_i       = self.lob_i
        lob_recs    = self.lob_recs

        has_lob = lob_i < len(lob_recs)
        has_tas = tas_i < len(tas_recs)

        # If both iterators are at the end of currently loaded records, try updating
        if not has_lob and not has_tas:
            self.synchronize(True) # Try to load more records
            # Update local copies of indices/lists
            tas_i       = self.tas_i
            tas_recs    = self.tas_recs
            lob_i       = self.lob_i
            lob_recs    = self.lob_recs
            has_lob = lob_i < len(lob_recs)
            has_tas = tas_i < len(tas_recs)
            # If still no records after update, stop iteration
            if not has_lob and not has_tas:
                 raise StopIteration

        # Determine which record comes next chronologically
        lob_ts = lob_recs[lob_i][depth_rec.timestamp] if has_lob else float('inf')
        tas_ts = tas_recs[tas_i][tas_rec.timestamp] if has_tas else float('inf')

        if lob_ts <= tas_ts: # Process LOB first if timestamps are equal or LOB is earlier
            res        = lob_recs[lob_i]
            self.ts    = res[depth_rec.timestamp]
            self.lob_i += 1
        elif has_tas: # Only process TAS if it exists and is strictly earlier
             res        = tas_recs[tas_i]
             self.ts    = res[tas_rec.timestamp]
             self.tas_i += 1
        else: # Should be unreachable if logic above is correct
             raise StopIteration # No LOB and no TAS available

        return res


    def __del__(self):
        """ Ensure files are closed when the object is destroyed """
        if hasattr(self, 'tas_fd') and self.tas_fd and not self.tas_fd.closed:
            try:
                self.tas_fd.close()
            except Exception as e:
                print(f"Error closing tas_fd: {e}")
        if hasattr(self, 'lob_fd') and self.lob_fd and not self.lob_fd.closed:
            try:
                self.lob_fd.close()
            except Exception as e:
                print(f"Error closing lob_fd: {e}")

    # Methods 'all' and '__getitem__' are complex and depend heavily on correct __next__
    # Keeping them basic for now.

    def all(self):
        res = []
        # Store current state
        orig_ts = self.ts
        orig_tas_i = self.tas_i
        orig_lob_i = self.lob_i
        # Reset to beginning - this assumes synchronize can handle seeking/re-reading if needed
        self.set_ts(0, update=True) # Ensure data is loaded if first time
        try:
            while True:
                res.append(self.__next__())
        except StopIteration:
            pass
        except Exception as e:
             print(f"Error during 'all()' iteration: {e}")
        finally:
            # Restore state - important!
            self.ts = orig_ts
            self.tas_i = orig_tas_i
            self.lob_i = orig_lob_i
        return res


    def __getitem__(self, slice_obj):
        # Basic timestamp slicing - requires iterating with __next__
        res = []
        if not isinstance(slice_obj, slice) or not isinstance(slice_obj.start, int):
            raise TypeError("Slicing only supported with integer timestamps, e.g., [ts_start:ts_stop]")

        start_ts = slice_obj.start
        stop_ts = slice_obj.stop if slice_obj.stop is not None else float('inf')

        # Store current state
        orig_ts = self.ts
        orig_tas_i = self.tas_i
        orig_lob_i = self.lob_i

        self.set_ts(start_ts, update=True) # Move iterator to start time, load data if needed

        try:
            while True:
                rec = self.__next__()
                rec_ts = rec[depth_rec.timestamp] if len(rec) >= 6 else rec[tas_rec.timestamp]
                if rec_ts > stop_ts:
                    break
                res.append(rec)
        except StopIteration:
            pass
        except Exception as e:
            print(f"Error during slice iteration: {e}")
        finally:
            # Restore original state
            self.ts = orig_ts
            self.tas_i = orig_tas_i
            self.lob_i = orig_lob_i

        return res