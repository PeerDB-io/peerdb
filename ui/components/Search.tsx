import { SearchField } from '@/lib/SearchField';
import { Dispatch, SetStateAction } from 'react';

interface SearchProps {
  allItems: any[];
  setItems: Dispatch<SetStateAction<any>>;
  filterFunction: (query: string) => {};
}
const SearchBar = ({ allItems, setItems, filterFunction }: SearchProps) => {
  const handleSearch = (searchQuery: string) => {
    if (searchQuery.length > 0) {
      setItems(filterFunction(searchQuery));
    }
    if (searchQuery.length == 0) {
      setItems(allItems);
    }
  };

  return (
    <SearchField
      placeholder='Search'
      onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
        handleSearch(e.target.value)
      }
    />
  );
};

export default SearchBar;
