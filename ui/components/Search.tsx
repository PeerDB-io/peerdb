import { SearchField } from '@/lib/SearchField';
import { Dispatch, SetStateAction, useEffect, useState } from 'react';

interface SearchProps {
  allItems: any[];
  setItems: Dispatch<SetStateAction<any>>;
  filterFunction: (query: string) => {};
}
const SearchBar = (props: SearchProps) => {
  const [searchQuery, setSearchQuery] = useState('');

  useEffect(() => {
    if (searchQuery.length > 0) {
      props.setItems(props.filterFunction(searchQuery));
    }
    if (searchQuery.length == 0) {
      props.setItems(props.allItems);
    }
  }, [searchQuery, props]);

  return (
    <SearchField
      placeholder='Search'
      value={searchQuery}
      onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
        setSearchQuery(e.target.value)
      }
    />
  );
};

export default SearchBar;
