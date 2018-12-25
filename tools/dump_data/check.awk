BEGIN{
  pre_value=0;
  count=0;
}
{
  if ((count != 0) && (count % 2 == 0))
  {
    before=substr(pre_value, length(pre_value) - 4);
    after=substr($3, length($3) - 4);
    if (int(before) != int(after) - 1)
    {
      print pre_value, $3, before, after;
    }
  }
  ++count;
  pre_value=$3;
}
END{
  print count;
}
